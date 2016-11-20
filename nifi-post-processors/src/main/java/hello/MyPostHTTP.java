/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hello;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.security.cert.X509Certificate;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.ManagedHttpClientConnection;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpCoreContext;
import org.apache.http.util.EntityUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.stream.io.BufferedOutputStream;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamThrottler;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.ObjectHolder;
import org.apache.nifi.util.StopWatch;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"http", "https", "remote", "copy", "archive"})
@CapabilityDescription("Performs an HTTP Post with the content of the FlowFile")
public class MyPostHTTP extends AbstractProcessor {

    public static final String CONTENT_TYPE_HEADER = "Content-Type";
    public static final String DEFAULT_CONTENT_TYPE = "application/octet-stream";
    public static final String FLOWFILE_CONFIRMATION_HEADER = "x-prefer-acknowledge-uri";
    public static final String LOCATION_HEADER_NAME = "Location";
    public static final String LOCATION_URI_INTENT_NAME = "x-location-uri-intent";
    public static final String LOCATION_URI_INTENT_VALUE = "flowfile-hold";
    
    public static final String PROTOCOL_VERSION_HEADER = "x-nifi-transfer-protocol-version";
    public static final String TRANSACTION_ID_HEADER = "x-nifi-transaction-id";
    public static final String PROTOCOL_VERSION = "3";

    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("URL")
            .description("The URL to POST to. The first part of the URL must be static. However, the path of the URL may be defined using the Attribute Expression Language. "
                    + "For example, https://${hostname} is not valid, but https://1.1.1.1:8080/files/${nf.file.name} is valid.")
            .required(true)
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("https?\\://.*")))
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connection Timeout")
            .description("How long to wait when attempting to connect to the remote server before giving up")
            .required(true)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATA_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Data Timeout")
            .description("How long to wait between receiving segments of data from the remote server before giving up and discarding the partial file")
            .required(true)
            .defaultValue("30 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that are successfully send will be transferred to success")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that fail to send will transferred to failure")
            .build();

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    private final AtomicReference<StreamThrottler> throttlerRef = new AtomicReference<>();
    private final ConcurrentMap<String, Config> configMap = new ConcurrentHashMap<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(URL);
        properties.add(SSL_CONTEXT_SERVICE);
        properties.add(CONNECTION_TIMEOUT);
        properties.add(DATA_TIMEOUT);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        if (context.getProperty(URL).getValue().startsWith("https") && context.getProperty(SSL_CONTEXT_SERVICE).getValue() == null) {
            results.add(new ValidationResult.Builder()
                    .explanation("URL is set to HTTPS protocol but no SSLContext has been specified")
                    .valid(false).subject("SSL Context").build());
        }
        return results;
    }

    @OnStopped
    public void onStopped() {

        for (final Map.Entry<String, Config> entry : configMap.entrySet()) {
            final Config config = entry.getValue();
            config.getConnectionManager().shutdown();
        }

        configMap.clear();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        this.throttlerRef.set(null);
    }

    private String getBaseUrl(final String url) {
        final int index = url.indexOf("/", 9);
        if (index < 0) {
            return url;
        }

        return url.substring(0, index);
    }

    private Config getConfig(final String url, final ProcessContext context) {
        final String baseUrl = getBaseUrl(url);
        Config config = configMap.get(baseUrl);
        if (config != null) {
            return config;
        }

        final PoolingHttpClientConnectionManager conMan;
        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sslContextService == null) {
            conMan = new PoolingHttpClientConnectionManager();
        } else {
            final SSLContext sslContext;
            try {
                sslContext = createSSLContext(sslContextService);
            } catch (final Exception e) {
                throw new ProcessException(e);
            }

            final SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, new String[]{"TLSv1"}, null, SSLConnectionSocketFactory.BROWSER_COMPATIBLE_HOSTNAME_VERIFIER);

            // Also use a plain socket factory for regular http connections (especially proxies)
            final Registry<ConnectionSocketFactory> socketFactoryRegistry =
                    RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("https", sslsf)
                            .register("http", PlainConnectionSocketFactory.getSocketFactory())
                            .build();

            conMan = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        }

        conMan.setDefaultMaxPerRoute(context.getMaxConcurrentTasks());
        conMan.setMaxTotal(context.getMaxConcurrentTasks());
        config = new Config(conMan);
        final Config existingConfig = configMap.putIfAbsent(baseUrl, config);

        return existingConfig == null ? config : existingConfig;
    }

    private SSLContext createSSLContext(final SSLContextService service)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException, KeyManagementException, UnrecoverableKeyException {
        SSLContextBuilder builder = SSLContexts.custom();
        final String trustFilename = service.getTrustStoreFile();
        if (trustFilename != null) {
            final KeyStore truststore = KeyStore.getInstance(service.getTrustStoreType());
            try (final InputStream in = new FileInputStream(new File(service.getTrustStoreFile()))) {
                truststore.load(in, service.getTrustStorePassword().toCharArray());
            }
            builder = builder.loadTrustMaterial(truststore, new TrustSelfSignedStrategy());
        }

        final String keyFilename = service.getKeyStoreFile();
        if (keyFilename != null) {
            final KeyStore keystore = KeyStore.getInstance(service.getKeyStoreType());
            try (final InputStream in = new FileInputStream(new File(service.getKeyStoreFile()))) {
                keystore.load(in, service.getKeyStorePassword().toCharArray());
            }
            builder = builder.loadKeyMaterial(keystore, service.getKeyStorePassword().toCharArray());
        }

        final SSLContext sslContext = builder.build();
        return sslContext;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
       
        final RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
        requestConfigBuilder.setConnectionRequestTimeout(context.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        requestConfigBuilder.setConnectTimeout(context.getProperty(CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        requestConfigBuilder.setRedirectsEnabled(false);
        requestConfigBuilder.setSocketTimeout(context.getProperty(DATA_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue());
        final RequestConfig requestConfig = requestConfigBuilder.build();

        final StreamThrottler throttler = throttlerRef.get();
        final ProcessorLog logger = getLogger();

        String lastUrl = null;
        long bytesToSend = 0L;

        final List<FlowFile> toSend = new ArrayList<>();
        CloseableHttpClient client = null;
        final String transactionId = UUID.randomUUID().toString();

        final ObjectHolder<String> dnHolder = new ObjectHolder<>("none");
        while (true) {
            FlowFile flowFile = session.get();
            if (flowFile == null) {
                break;
            }

            final String url = context.getProperty(URL).evaluateAttributeExpressions(flowFile).getValue();
            try {
                new java.net.URL(url);
            } catch (final MalformedURLException e) {
                logger.error("After substituting attribute values for {}, URL is {}; this is not a valid URL, so routing to failure",
                        new Object[]{flowFile, url});
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                continue;
            }

            // If this FlowFile doesn't have the same url, throw it back on the queue and stop grabbing FlowFiles
            if (lastUrl != null && !lastUrl.equals(url)) {
                session.transfer(flowFile);
                break;
            }

            lastUrl = url;
            toSend.add(flowFile);

            if (client == null) {
                final Config config = getConfig(url, context);
                final HttpClientConnectionManager conMan = config.getConnectionManager();

                final HttpClientBuilder clientBuilder = HttpClientBuilder.create();
                clientBuilder.setConnectionManager(conMan);
                clientBuilder.addInterceptorFirst(new HttpResponseInterceptor() {
                    @Override
                    public void process(final HttpResponse response, final HttpContext httpContext) throws HttpException, IOException {
                        final HttpCoreContext coreContext = HttpCoreContext.adapt(httpContext);
                        final ManagedHttpClientConnection conn = coreContext.getConnection(ManagedHttpClientConnection.class);
                        if (!conn.isOpen()) {
                            return;
                        }

                        final SSLSession sslSession = conn.getSSLSession();

                        if (sslSession != null) {
                            final X509Certificate[] certChain = sslSession.getPeerCertificateChain();
                            if (certChain == null || certChain.length == 0) {
                                throw new SSLPeerUnverifiedException("No certificates found");
                            }

                            final X509Certificate cert = certChain[0];
                            dnHolder.set(cert.getSubjectDN().getName().trim());
                        }
                    }
                });

                clientBuilder.disableAutomaticRetries();
                clientBuilder.disableContentCompression();

                client = clientBuilder.build();
            }

            bytesToSend += flowFile.getSize();
            break;
        }

        if (toSend.isEmpty()) {
            return;
        }

        final String url = lastUrl;
        final HttpPost post = new HttpPost(url);
        final List<FlowFile> flowFileList = toSend;
        
        String userName = "Chris";
        String password = "password";
        final MultipartEntityBuilder builder = MultipartEntityBuilder.create();
        builder.addTextBody("userName", userName);
        builder.addTextBody("password", password);
        for (final FlowFile flowFile : flowFileList) {
        	session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream rawIn) throws IOException {
            	InputStream in = new ByteArrayInputStream(IOUtils.toByteArray(rawIn));
            	builder.addBinaryBody("file", in,  ContentType.DEFAULT_BINARY , "filename"); 
            }
        	});
        }
        
        final HttpEntity entity2 = builder.build();
        
        post.setEntity(entity2);        
        post.setConfig(requestConfig);

        final String contentType;

        contentType = DEFAULT_CONTENT_TYPE;
        post.setHeader(CONTENT_TYPE_HEADER, contentType);
        post.setHeader(FLOWFILE_CONFIRMATION_HEADER, "true");
        post.setHeader(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION);
        post.setHeader(TRANSACTION_ID_HEADER, transactionId);

        // Do the actual POST
        final String flowFileDescription = toSend.size() <= 10 ? toSend.toString() : toSend.size() + " FlowFiles";

        final String uploadDataRate;
        final long uploadMillis;
        CloseableHttpResponse response = null;
        try {
            final StopWatch stopWatch = new StopWatch(true);
            response = client.execute(post);
            // consume input stream entirely, ignoring its contents. If we
            // don't do this, the Connection will not be returned to the pool
            EntityUtils.consume(response.getEntity());
            stopWatch.stop();
            uploadDataRate = stopWatch.calculateDataRate(bytesToSend);
            uploadMillis = stopWatch.getDuration(TimeUnit.MILLISECONDS);
        } catch (final IOException e) {
        	logger.error("Failed to Post {} due to {}; transferring to failure", new Object[]{flowFileDescription, e});
            context.yield();
            for (FlowFile flowFile : toSend) {
                flowFile = session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
            }
            return;
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (final IOException e) {
                    getLogger().warn("Failed to close HTTP Response due to {}", new Object[]{e});
                }
            }
        }

        // If we get a 'SEE OTHER' status code and an HTTP header that indicates that the intent
        // of the Location URI is a flowfile hold, we will store this holdUri. This prevents us
        // from posting to some other webservice and then attempting to delete some resource to which
        // we are redirected
        final int responseCode = response.getStatusLine().getStatusCode();
        final String responseReason = response.getStatusLine().getReasonPhrase();
        String holdUri = null;
        if (responseCode == HttpServletResponse.SC_SEE_OTHER) {
            final Header locationUriHeader = response.getFirstHeader(LOCATION_URI_INTENT_NAME);
            if (locationUriHeader != null) {
                if (LOCATION_URI_INTENT_VALUE.equals(locationUriHeader.getValue())) {
                    final Header holdUriHeader = response.getFirstHeader(LOCATION_HEADER_NAME);
                    if (holdUriHeader != null) {
                        holdUri = holdUriHeader.getValue();
                    }
                }
            }

            if (holdUri == null) {
                for (FlowFile flowFile : toSend) {
                    flowFile = session.penalize(flowFile);
                    logger.error("Failed to Post {} to {}: sent content and received status code {}:{} but no Hold URI",
                            new Object[]{flowFile, url, responseCode, responseReason});
                    session.transfer(flowFile, REL_FAILURE);
                }
                return;
            }
        }

        if (holdUri == null) {
            if (responseCode == HttpServletResponse.SC_SERVICE_UNAVAILABLE) {
                for (FlowFile flowFile : toSend) {
                    flowFile = session.penalize(flowFile);
                    logger.error("Failed to Post {} to {}: response code was {}:{}; will yield processing, "
                            + "since the destination is temporarily unavailable",
                            new Object[]{flowFile, url, responseCode, responseReason});
                    session.transfer(flowFile, REL_FAILURE);
                }
                context.yield();
                return;
            }

            if (responseCode >= 300) {
                for (FlowFile flowFile : toSend) {
                    flowFile = session.penalize(flowFile);
                    logger.error("Failed to Post {} to {}: response code was {}:{}",
                            new Object[]{flowFile, url, responseCode, responseReason});
                    session.transfer(flowFile, REL_FAILURE);
                }
                return;
            }

            logger.info("Successfully Posted {} to {} in {} at a rate of {}",
                    new Object[]{flowFileDescription, url, FormatUtils.formatMinutesSeconds(uploadMillis, TimeUnit.MILLISECONDS), uploadDataRate});

            for (final FlowFile flowFile : toSend) {
                session.getProvenanceReporter().send(flowFile, url, "Remote DN=" + dnHolder.get(), uploadMillis, true);
                session.transfer(flowFile, REL_SUCCESS);
            }
            return;
        }

        //
        // the response indicated a Hold URI; delete the Hold.
        //
        // determine the full URI of the Flow File's Hold; Unfortunately, the responses that are returned have
        // changed over the past, so we have to take into account a few different possibilities.
        String fullHoldUri = holdUri;
        if (holdUri.startsWith("/contentListener")) {
            // If the Hold URI that we get starts with /contentListener, it may not really be /contentListener,
            // as this really indicates that it should be whatever we posted to -- if posting directly to the
            // ListenHTTP component, it will be /contentListener, but if posting to a proxy/load balancer, we may
            // be posting to some other URL.
            fullHoldUri = url + holdUri.substring(16);
        } else if (holdUri.startsWith("/")) {
            // URL indicates the full path but not hostname or port; use the same hostname & port that we posted
            // to but use the full path indicated by the response.
            int firstSlash = url.indexOf("/", 8);
            if (firstSlash < 0) {
                firstSlash = url.length();
            }
            final String beforeSlash = url.substring(0, firstSlash);
            fullHoldUri = beforeSlash + holdUri;
        } else if (!holdUri.startsWith("http")) {
            // Absolute URL
            fullHoldUri = url + (url.endsWith("/") ? "" : "/") + holdUri;
        }

        final HttpDelete delete = new HttpDelete(fullHoldUri);
        delete.setHeader(TRANSACTION_ID_HEADER, transactionId);

        while (true) {
            try {
                final HttpResponse holdResponse = client.execute(delete);
                EntityUtils.consume(holdResponse.getEntity());
                final int holdStatusCode = holdResponse.getStatusLine().getStatusCode();
                final String holdReason = holdResponse.getStatusLine().getReasonPhrase();
                if (holdStatusCode >= 300) {
                    logger.error("Failed to delete Hold that destination placed on {}: got response code {}:{}; routing to failure",
                            new Object[]{flowFileDescription, holdStatusCode, holdReason});

                    for (FlowFile flowFile : toSend) {
                        flowFile = session.penalize(flowFile);
                        session.transfer(flowFile, REL_FAILURE);
                    }
                    return;
                }

                logger.info("Successfully Posted {} to {} in {} milliseconds at a rate of {}", new Object[]{flowFileDescription, url, uploadMillis, uploadDataRate});

                for (final FlowFile flowFile : toSend) {
                    session.getProvenanceReporter().send(flowFile, url);
                    session.transfer(flowFile, REL_SUCCESS);
                }
                return;
            } catch (final IOException e) {
                logger.warn("Failed to delete Hold that destination placed on {} due to {}", new Object[]{flowFileDescription, e});
            }

            if (!isScheduled()) {
                context.yield();
                logger.warn("Failed to delete Hold that destination placed on {}; Processor has been stopped so routing FlowFile(s) to failure", new Object[]{flowFileDescription});
                for (FlowFile flowFile : toSend) {
                    flowFile = session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                }
                return;
            }
        }
    }

    private static class Config {

        private final HttpClientConnectionManager conMan;

        public Config(final HttpClientConnectionManager conMan) {
            this.conMan = conMan;
        }

        public HttpClientConnectionManager getConnectionManager() {
            return conMan;
        }
    }
}