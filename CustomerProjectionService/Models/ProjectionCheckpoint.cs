namespace CustomerProjectionService.Models;

public class ProjectionCheckpoint
{
    public string Id { get; set; } = string.Empty;
    public ulong CommitPosition { get; set; }
    public ulong PreparePosition { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}