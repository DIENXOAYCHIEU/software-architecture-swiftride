using TripService.Domain.ValueObjects;
using TripService.Domain.Enums;

namespace TripService.Application.DTOs;

public class AssignedTripResponse
{
    public Guid Id { get; set; }
    public Guid RiderId { get; set; }
    public Guid? DriverId { get; set; }
    public Location StartLocation { get; set; } = default!;
    public Location EndLocation { get; set; } = default!;
    public TripStatus Status { get; set; }

    public AssignedTripResponse() { }

    public AssignedTripResponse(
        Guid id,
        Guid riderId,
        Guid? driverId,
        Location startLocation,
        Location endLocation,
        TripStatus status)
    {
        Id = id;
        RiderId = riderId;
        DriverId = driverId;
        StartLocation = startLocation;
        EndLocation = endLocation;
        Status = status;
    }
}
