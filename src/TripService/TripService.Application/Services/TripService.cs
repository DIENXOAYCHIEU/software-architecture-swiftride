using TripService.Application.Interfaces;
using TripService.Application.DTOs;
using TripService.Domain.Entities;
using TripService.Domain.Exceptions;

namespace TripService.Application.Services;

public class TripService : ITripService
{
    private readonly ITripRepository _tripRepository;
    private readonly IUnitOfWork _unitOfWork;
    private readonly ITripEventPublisher _events;

    public TripService(ITripRepository tripRepository, IUnitOfWork unitOfWork, ITripEventPublisher events)
    {
        _tripRepository = tripRepository;
        _unitOfWork = unitOfWork;
        _events = events;
    }

    public async Task<Trip> GetByIdAsync(Guid tripId)
    {
        var trip = await _tripRepository.GetByIdAsync(tripId);
        if (trip == null)
        {
            throw new InvalidTripException($"Chuyến đi {tripId} không tồn tại.");
        }
        return trip;
    }

    public async Task<CreatedTripResponse> CreateTripAsync(CreatedTripRequest request)
    {
        var trip = new Trip(
            request.RiderId,
            request.StartLocation,
            request.EndLocation,
            DateTime.UtcNow
        );

        await _tripRepository.AddAsync(trip);

        // Bắn payload khớp 100% với TripCreatedPayload bên MatchingService
        await _events.PublishAsync("TripCreated", new
        {
            TripId = trip.Id,
            PickupLatitude = trip.StartLocation.Latitude,
            PickupLongitude = trip.StartLocation.Longitude,
            DropoffLatitude = trip.EndLocation.Latitude,
            DropoffLongitude = trip.EndLocation.Longitude
        });

        await _unitOfWork.SaveChangesAsync();

        return new CreatedTripResponse(
            trip.Id,
            trip.RiderId,
            trip.StartLocation,
            trip.EndLocation,
            trip.Status
        );
    }

    public async Task<AssignedTripResponse> AssignDriverAsync(Guid tripId, Guid driverId)
    {
        var trip = await _tripRepository.GetByIdAsync(tripId)
            ?? throw new InvalidTripException("Chuyến đi không tồn tại");

        trip.AssignDriver(driverId);
        await _unitOfWork.SaveChangesAsync();

        return MapToAssignedResponse(trip);
    }

    public async Task<AssignedTripResponse> UpdateStatusToPicked(Guid tripId)
    {
        var trip = await _tripRepository.GetByIdAsync(tripId)
            ?? throw new InvalidTripException("Chuyến đi không tồn tại");

        trip.PickRiderUp();
        await _unitOfWork.SaveChangesAsync();

        return MapToAssignedResponse(trip);
    }

    public async Task<AssignedTripResponse> UpdateStatusToDropped(Guid tripId)
    {
        var trip = await _tripRepository.GetByIdAsync(tripId)
            ?? throw new InvalidTripException("Chuyến đi không tồn tại");

        trip.DropRiderOff();
        trip.MarkPaymentPending();

        // Phải gọi PublishAsync TRƯỚC SaveChangesAsync để lưu vào OutboxMessages
        await _events.PublishAsync("RideCompleted", new
        {
            TripId = trip.Id,
            RiderId = trip.RiderId,
            Amount = trip.AmountToPay ?? 85000m
        });

        await _unitOfWork.SaveChangesAsync();

        return MapToAssignedResponse(trip);
    }

    public async Task<AssignedTripResponse> UpdatePaymentStatusAsync(Guid tripId, bool paid)
    {
        var trip = await _tripRepository.GetByIdAsync(tripId)
            ?? throw new InvalidTripException("Chuyến đi không tồn tại");

        if (paid) trip.MarkPaid();
        else trip.MarkPaymentPending();

        await _unitOfWork.SaveChangesAsync();

        return MapToAssignedResponse(trip);
    }

    public async Task<AssignedTripResponse> CancelTripAsync(Guid tripId, string reason)
    {
        var trip = await _tripRepository.GetByIdAsync(tripId)
            ?? throw new InvalidTripException("Chuyến đi không tồn tại");

        trip.Cancel();

        // Phải gọi PublishAsync TRƯỚC SaveChangesAsync để lưu vào OutboxMessages
        await _events.PublishAsync("TripCompensationRequested", new
        {
            TripId = trip.Id,
            Reason = reason
        });

        await _unitOfWork.SaveChangesAsync();

        return MapToAssignedResponse(trip);
    }

    private static AssignedTripResponse MapToAssignedResponse(Trip trip)
    {
        return new AssignedTripResponse(
            trip.Id,
            trip.RiderId,
            trip.DriverId,
            trip.StartLocation,
            trip.EndLocation,
            trip.Status
        );
    }
}
