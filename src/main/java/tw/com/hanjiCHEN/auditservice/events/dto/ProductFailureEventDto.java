package tw.com.hanjiCHEN.auditservice.events.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ProductFailureEventDto(
        String email,
        int status,
        String error,
        String id

) {

}

