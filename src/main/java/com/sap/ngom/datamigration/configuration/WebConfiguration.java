/*
 * Copyright (c) 2017 SAP SE or an SAP affiliate company. All rights reserved.
 *//*

package com.sap.ngom.datamigration.configuration;

import com.sap.hcp.cf.logging.common.LogContext;
import com.sap.hcp.cf.logging.servlet.filter.RequestLoggingFilter;
import com.sap.ngom.util.headers.NgomHeaderConstants;
import com.sap.ngom.util.headers.NgomHeaderFacade;
import com.sap.ngom.util.headers.http.NgomHeaderRequestInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.filter.CommonsRequestLoggingFilter;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

@Configuration
public class WebConfiguration implements WebMvcConfigurer {
    @Autowired
    private NgomHeaderRequestInterceptor ngomHeaderRequestInterceptor;

    @Autowired
    private NgomHeaderFacade ngomHeaderFacade;

    @Override
    public void addInterceptors(final InterceptorRegistry registry) {
    }

    @Bean
    public FilterRegistrationBean hcpCfRequestLoggingFilter() {
        final FilterRegistrationBean registration = new FilterRegistrationBean();
        final Filter filter = new DataMigrationCfRequestLoggingFilter();
        registration.setFilter(filter);
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return registration;
    }

    @Bean
    public FilterRegistrationBean commonsRequestLoggingFilter() {
        final FilterRegistrationBean registration = new FilterRegistrationBean();
        final DataMigrationRequestLoggingFilter commonsRequestLoggingFilter = new DataMigrationRequestLoggingFilter();
        commonsRequestLoggingFilter.setIncludeClientInfo(true);
        commonsRequestLoggingFilter.setIncludeQueryString(true);
        commonsRequestLoggingFilter.setIncludePayload(true);
        commonsRequestLoggingFilter.setIncludeHeaders(false);
        commonsRequestLoggingFilter.setMaxPayloadLength(10000);

        registration.setFilter(commonsRequestLoggingFilter);
        registration.setOrder(Ordered.LOWEST_PRECEDENCE);
        return registration;
    }

    public class DataMigrationRequestLoggingFilter extends CommonsRequestLoggingFilter {
        @Override
        protected String createMessage(final HttpServletRequest request, final String prefix, final String suffix) {
            final String msg = super.createMessage(request, prefix, suffix);
            final HttpHeaders headers = new ServletServerHttpRequest(request).getHeaders();
            headers.keySet().removeIf(
                    key -> ngomHeaderFacade.isBlacklisted(key) || key.equalsIgnoreCase(HttpHeaders.ACCEPT_CHARSET));
            return msg + (";headers=" + headers);
        }

        @Override
        protected boolean shouldLog(HttpServletRequest request) {
            // do not log actuator health requests
            if (request.getRequestURI().contains("/admin/health")) {
                return false;
            } else {
                return super.shouldLog(request);
            }
        }
    }

    public static class DataMigrationCfRequestLoggingFilter extends RequestLoggingFilter {

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                throws IOException, ServletException {
            addHeaderToLogContext((HttpServletRequest) request);
            super.doFilter(request, response, chain);
        }

        private void addHeaderToLogContext(final HttpServletRequest request) {
            addHeaderToLogContext(request, NgomHeaderConstants.NGOM_TENANT, "tenant");
        }

        private void addHeaderToLogContext(final HttpServletRequest request, final String headerName,
                                           final String logContextName) {
            if (request.getHeader(headerName) == null) {
                LogContext.remove(logContextName);
            } else {
                LogContext.add(logContextName, request.getHeader(headerName));
            }
        }
    }

    public static class DataMigrationRequestInterceptor implements ClientHttpRequestInterceptor {
        @Override
        public ClientHttpResponse intercept(final HttpRequest request, final byte[] body,
                                            final ClientHttpRequestExecution execution) throws IOException {
            final HttpRequest httpRequestWrapper = new HttpRequestWrapper(request);
            httpRequestWrapper.getHeaders().set(LogContext.HTTP_HEADER_CORRELATION_ID, LogContext.getCorrelationId());
            return execution.execute(httpRequestWrapper, body);
        }
    }
}
*/
