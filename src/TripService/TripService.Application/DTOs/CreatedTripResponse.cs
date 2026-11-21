using TripService.Domain.ValueObjects;
using TripService.Domain.Enums;

namespace TripService.Application.DTOs;

public class CreatedTripResponse
{
    public Guid Id { get; set; }
    public Guid RiderId { get; set; }
    public Location StartLocation { get; set; } = default!;
    public Location EndLocation { get; set; } = default!;
    public TripStatus Status { get; set; }

    public CreatedTripResponse() { }

    public CreatedTripResponse(
        Guid id,
        Guid riderId,
        Location startLocation,
        Location endLocation,
        TripStatus status)
    {
        Id = id;
        RiderId = riderId;
        StartLocation = startLocation;
        EndLocation = endLocation;
        Status = status;
    }
}
