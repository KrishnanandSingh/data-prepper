/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;
import org.opensearch.dataprepper.peerforwarder.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.peerforwarder.client.PeerForwarderClient;
import org.opensearch.dataprepper.peerforwarder.codec.JacksonPeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.codec.JavaPeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.codec.PeerForwarderCodec;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpServerProvider;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderHttpService;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServer;
import org.opensearch.dataprepper.peerforwarder.server.PeerForwarderServerProxy;
import org.opensearch.dataprepper.peerforwarder.server.ResponseHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.yaml.snakeyaml.LoaderOptions;

@Configuration
class PeerForwarderAppConfig {
    static final String COMPONENT_SCOPE = "core";
    static final String COMPONENT_ID = "peerForwarder";

    @Bean(name = "peerForwarderMetrics")
    public PluginMetrics pluginMetrics() {
        return PluginMetrics.fromNames(COMPONENT_ID, COMPONENT_SCOPE);
    }

    @Bean(name = "peerForwarderObjectMapper")
    public ObjectMapper objectMapper() {
        final JavaTimeModule javaTimeModule = new JavaTimeModule();
        final LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setCodePointLimit(10 * 1024 * 1024); // 10MB
        final YAMLFactory yamlFactory = YAMLFactory.builder()
                .loaderOptions(loaderOptions)
                .build();
        return new ObjectMapper(yamlFactory).registerModule(javaTimeModule);
    }

    @Bean
    public PeerForwarderConfiguration peerForwarderConfiguration(
            @Autowired(required = false) final DataPrepperConfiguration dataPrepperConfiguration) {
        if (dataPrepperConfiguration != null && dataPrepperConfiguration.getPeerForwarderConfiguration() != null) {
                return dataPrepperConfiguration.getPeerForwarderConfiguration();
            }
        else
            return new PeerForwarderConfiguration();
    }

    @Bean
    public PeerClientPool peerClientPool() {
        return new PeerClientPool();
    }

    @Bean
    public CertificateProviderFactory certificateProviderFactory(final PeerForwarderConfiguration peerForwarderConfiguration) {
        return new CertificateProviderFactory(peerForwarderConfiguration);
    }

    @Bean
    public PeerForwarderClientFactory peerForwarderClientFactory(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final PeerClientPool peerClientPool,
            final CertificateProviderFactory certificateProviderFactory,
            @Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics
    ) {
        return new PeerForwarderClientFactory(peerForwarderConfiguration, peerClientPool, certificateProviderFactory, pluginMetrics);
    }

    @Bean
    public PeerForwarderClient peerForwarderClient(final PeerForwarderConfiguration peerForwarderConfiguration,
                                                   final PeerForwarderClientFactory peerForwarderClientFactory,
                                                   final PeerForwarderCodec peerForwarderCodec,
                                                   @Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics
    ) {
        return new PeerForwarderClient(
                peerForwarderConfiguration, peerForwarderClientFactory, peerForwarderCodec, pluginMetrics);
    }

    @Bean
    public PeerForwarderProvider peerForwarderProvider(final PeerForwarderClientFactory peerForwarderClientFactory,
                                                       final PeerForwarderClient peerForwarderClient,
                                                       final PeerForwarderConfiguration peerForwarderConfiguration,
                                                       @Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics) {
        return new PeerForwarderProvider(peerForwarderClientFactory, peerForwarderClient, peerForwarderConfiguration, pluginMetrics);
    }

    @Bean
    public ResponseHandler responseHandler(@Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics) {
        return new ResponseHandler(pluginMetrics);
    }

    @Bean
    public PeerForwarderCodec peerForwarderCodec(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            @Qualifier("peerForwarderObjectMapper") final ObjectMapper objectMapper) {
        return peerForwarderConfiguration.getBinaryCodec() ?
                new JavaPeerForwarderCodec() : new JacksonPeerForwarderCodec(objectMapper);
    }

    @Bean
    public PeerForwarderHttpService peerForwarderHttpService(
            final ResponseHandler responseHandler,
            final PeerForwarderProvider peerForwarderProvider,
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final PeerForwarderCodec peerForwarderCodec,
            @Qualifier("peerForwarderMetrics") final PluginMetrics pluginMetrics
    ) {
        return new PeerForwarderHttpService(responseHandler, peerForwarderProvider, peerForwarderConfiguration,
                peerForwarderCodec, pluginMetrics);
    }

    @Bean
    public PeerForwarderHttpServerProvider peerForwarderHttpServerProvider(
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final CertificateProviderFactory certificateProviderFactory,
            final PeerForwarderHttpService peerForwarderHttpService
    ) {
        return new PeerForwarderHttpServerProvider(peerForwarderConfiguration,
                certificateProviderFactory, peerForwarderHttpService);
    }

    @Bean
    public PeerForwarderServer peerForwarderServer(
            final PeerForwarderHttpServerProvider peerForwarderHttpServerProvider,
            final PeerForwarderConfiguration peerForwarderConfiguration,
            final PeerForwarderProvider peerForwarderProvider) {
        return new PeerForwarderServerProxy(peerForwarderHttpServerProvider, peerForwarderConfiguration, peerForwarderProvider);
    }

}
