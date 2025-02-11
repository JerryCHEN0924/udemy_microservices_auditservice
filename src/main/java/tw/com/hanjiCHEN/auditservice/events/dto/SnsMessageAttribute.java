package tw.com.hanjiCHEN.auditservice.events.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SnsMessageAttribute(
        @JsonProperty("Type")
        String type,
        @JsonProperty("Value")
        String value
) {
}
