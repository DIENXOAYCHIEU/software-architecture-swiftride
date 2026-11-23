using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using TripService.Application.Interfaces;
using TripService.Infrastructure.Persistence;

namespace TripService.Infrastructure.Integration;

public sealed class RabbitMqOptions
{
    public string Host { get; init; } = "localhost";
    public int Port { get; init; } = 5672;
    public string UserName { get; init; } = "guest";
    public string Password { get; init; } = "guest";
    public string Exchange { get; init; } = "swiftride.events";
    public string Queue { get; init; } = "trip.events";
    public string DeadLetterExchange { get; init; } = "swiftride.events.dlx";
    public string DeadLetterQueue { get; init; } = "trip.events.dlq";
}

public sealed class OutboxMessage
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public required string EventName { get; set; }
    public required string Payload { get; set; }
    public DateTime OccurredAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime? PublishedAtUtc { get; set; }
}

public sealed class ProcessedIntegrationMessage
{
    public required string MessageId { get; set; }
    public DateTime ProcessedAtUtc { get; set; } = DateTime.UtcNow;
}

public sealed class RabbitMqConnection(RabbitMqOptions options) : IDisposable
{
    private readonly IConnection connection = new ConnectionFactory
    {
        HostName = options.Host,
        Port = options.Port,
        UserName = options.UserName,
        Password = options.Password,
        DispatchConsumersAsync = true,
        AutomaticRecoveryEnabled = true
    }.CreateConnection();

    public IModel CreateChannel() => connection.CreateModel();
    public void Dispose() => connection.Dispose();
}

public sealed class TripEventPublisher(TripDbContext db) : ITripEventPublisher
{
    public Task PublishAsync(string eventName, object payload, CancellationToken ct = default)
    {
        db.OutboxMessages.Add(new OutboxMessage
        {
            EventName = eventName,
            Payload = JsonSerializer.Serialize(payload)
        });
        return Task.CompletedTask;
    }
}

public sealed class TripOutboxWorker(
    IServiceScopeFactory scopes,
    RabbitMqConnection rabbit,
    RabbitMqOptions options,
    ILogger<TripOutboxWorker> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = scopes.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<TripDbContext>();
                var messages = await db.OutboxMessages
                    .Where(x => x.PublishedAtUtc == null)
                    .OrderBy(x => x.OccurredAtUtc)
                    .Take(20)
                    .ToListAsync(stoppingToken);

                if (messages.Count > 0)
                {
                    using var channel = rabbit.CreateChannel();
                    channel.ExchangeDeclare(options.Exchange, ExchangeType.Topic, durable: true);

                    var props = channel.CreateBasicProperties();
                    props.Persistent = true;

                    foreach (var message in messages)
                    {
                        var body = JsonSerializer.Serialize(new
                        {
                            MessageId = message.Id.ToString(),
                            message.EventName,
                            message.Payload
                        });

                        channel.BasicPublish(
                            options.Exchange,
                            message.EventName,
                            props,
                            Encoding.UTF8.GetBytes(body));

                        message.PublishedAtUtc = DateTime.UtcNow;
                    }

                    await db.SaveChangesAsync(stoppingToken);
                }
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Trip outbox iteration failed");
            }

            await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
        }
    }
}

public sealed class TripPaymentConsumer(
    IServiceScopeFactory scopes,
    RabbitMqConnection rabbit,
    RabbitMqOptions options,
    ILogger<TripPaymentConsumer> log) : BackgroundService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    private IModel? _channel;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _channel = rabbit.CreateChannel();
        _channel.ExchangeDeclare(options.Exchange, ExchangeType.Topic, durable: true);
        _channel.ExchangeDeclare(options.DeadLetterExchange, ExchangeType.Topic, durable: true);
        _channel.QueueDeclare(options.DeadLetterQueue, durable: true, exclusive: false, autoDelete: false);
        _channel.QueueBind(options.DeadLetterQueue, options.DeadLetterExchange, "#");

        var queueArguments = new Dictionary<string, object>
        {
            ["x-dead-letter-exchange"] = options.DeadLetterExchange,
            ["x-dead-letter-routing-key"] = options.DeadLetterQueue
        };

        _channel.QueueDeclare(options.Queue, durable: true, exclusive: false, autoDelete: false, queueArguments);

        // Giới hạn xử lý 1 message tại 1 thời điểm để đảm bảo an toàn thread cho channel
        _channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);

        // Đăng ký nhận event từ MatchingService & PricingService
        _channel.QueueBind(options.Queue, options.Exchange, "DriverAssigned");
        _channel.QueueBind(options.Queue, options.Exchange, "PriceCalculated");

        // Đăng ký nhận event từ PaymentService
        _channel.QueueBind(options.Queue, options.Exchange, "PaymentSettled");
        _channel.QueueBind(options.Queue, options.Exchange, "PaymentFailed");
        _channel.QueueBind(options.Queue, options.Exchange, "PaymentRefunded");

        var consumer = new AsyncEventingBasicConsumer(_channel);
        consumer.Received += async (_, args) =>
        {
            try
            {
                var envelope = JsonSerializer.Deserialize<Envelope>(
                    Encoding.UTF8.GetString(args.Body.ToArray()), JsonOptions)
                    ?? throw new InvalidOperationException("Invalid integration envelope.");

                using var scope = scopes.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<TripDbContext>();

                // Chống xử lý trùng message (Idempotency)
                if (await db.ProcessedIntegrationMessages.AnyAsync(
                        x => x.MessageId == envelope.MessageId, stoppingToken))
                {
                    _channel.BasicAck(args.DeliveryTag, false);
                    return;
                }

                if (envelope.EventName == "DriverAssigned")
                {
                    var e = JsonSerializer.Deserialize<DriverAssignedEvent>(envelope.Payload, JsonOptions)
                        ?? throw new InvalidOperationException("Invalid DriverAssigned payload.");
                    var trip = await db.Trips.FirstOrDefaultAsync(
                        x => x.Id == e.TripId, stoppingToken)
                        ?? throw new InvalidOperationException("Trip not found.");

                    if (trip.DriverId != e.DriverId) trip.AssignDriver(e.DriverId);
                }
                else if (envelope.EventName == "PriceCalculated")
                {
                    var e = JsonSerializer.Deserialize<PriceCalculatedEvent>(envelope.Payload, JsonOptions)
                        ?? throw new InvalidOperationException("Invalid PriceCalculated payload.");
                    var trip = await db.Trips.FirstOrDefaultAsync(
                        x => x.Id == e.TripId, stoppingToken)
                        ?? throw new InvalidOperationException("Trip not found.");

                    trip.AmountToPay = e.FinalPrice;
                }
                else if (envelope.EventName is "PaymentSettled" or "PaymentFailed" or "PaymentRefunded")
                {
                    var payment = JsonSerializer.Deserialize<PaymentEvent>(envelope.Payload, JsonOptions)
                        ?? throw new InvalidOperationException("Invalid payment payload.");
                    var trip = await db.Trips.FirstOrDefaultAsync(
                        x => x.Id == payment.TripId, stoppingToken)
                        ?? throw new InvalidOperationException("Trip for payment event was not found.");

                    if (envelope.EventName == "PaymentSettled") trip.MarkPaid();
                    else if (envelope.EventName == "PaymentFailed") trip.MarkPaymentPending();
                    else if (envelope.EventName == "PaymentRefunded") trip.Cancel();
                }
                else
                {
                    log.LogWarning("Unhandled event {EventName} with MessageId {MessageId}", envelope.EventName, envelope.MessageId);
                }

                db.ProcessedIntegrationMessages.Add(new ProcessedIntegrationMessage
                {
                    MessageId = envelope.MessageId
                });

                await db.SaveChangesAsync(stoppingToken);
                _channel.BasicAck(args.DeliveryTag, false);
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Trip integration event failed");
                _channel?.BasicNack(args.DeliveryTag, false, false);
            }
        };

        _channel.BasicConsume(options.Queue, false, consumer);
        return Task.CompletedTask;
    }

    public override void Dispose()
    {
        _channel?.Close();
        _channel?.Dispose();
        base.Dispose();
    }

    private sealed record Envelope(string MessageId, string EventName, string Payload);
    private sealed record DriverAssignedEvent(Guid TripId, Guid DriverId);
    private sealed record PaymentEvent(Guid PaymentId, Guid TripId, decimal Amount);
    private sealed record PriceCalculatedEvent(Guid TripId, decimal FinalPrice);
}

public static class IntegrationRegistration
{
    public static IServiceCollection AddInfrastructureIntegration(
        this IServiceCollection services, RabbitMqOptions options)
    {
        services.AddScoped<ITripEventPublisher, TripEventPublisher>();
        services.AddSingleton(options);
        services.AddSingleton<RabbitMqConnection>();
        services.AddHostedService<TripOutboxWorker>();
        services.AddHostedService<TripPaymentConsumer>();
        return services;
    }
}
