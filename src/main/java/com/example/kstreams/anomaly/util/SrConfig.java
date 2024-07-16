package com.example.kstreams.anomaly.util;

import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Optional;

@Dependent
public class SrConfig {

    public String srUrl;
    public Optional<String> basicAuthUserInfo;

    @Inject
    public SrConfig(@ConfigProperty(name = "schema.registry.url", defaultValue = "http://localhost:8081") String srUrl,
                    @ConfigProperty(name = "basic.auth.user.info") Optional<String> basicAuthUserInfo
    ) {
        this.srUrl = srUrl;
        this.basicAuthUserInfo = basicAuthUserInfo;
    }


}
