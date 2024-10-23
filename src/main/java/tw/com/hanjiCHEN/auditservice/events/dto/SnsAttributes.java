package tw.com.hanjiCHEN.auditservice.events.dto;

public record SnsAttributes(
        SnsMessageAttribute traceId,
        SnsMessageAttribute eventType,
        SnsMessageAttribute requestId
) {
}
