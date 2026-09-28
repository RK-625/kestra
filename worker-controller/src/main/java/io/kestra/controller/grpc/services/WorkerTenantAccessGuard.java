package io.kestra.controller.grpc.services;

import java.util.List;
import java.util.function.Function;

import io.kestra.controller.grpc.RequestOrResponseHeader;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import jakarta.inject.Singleton;

/**
 * Checks the tenant of the data a worker writes through a gRPC call whose payload is only known once
 * decoded, and which therefore cannot be checked by a server interceptor. Both methods are called on
 * the gRPC call's thread, after the payload has been deserialized. The default (OSS) implementation
 * applies no restriction; EE restricts a worker to the tenants it is allowed to serve.
 */
public interface WorkerTenantAccessGuard {

    /**
     * @param records the decoded records of a batch
     * @param tenantOf returns the tenant a record is written to
     * @return the records the calling worker may write and the ones it may not, each in their original order
     */
    <T> Partition<T> partition(RequestOrResponseHeader header, List<T> records, Function<T, String> tenantOf);

    /**
     * @param declaredTenantId the tenant declared on the request, empty when sent by a worker that predates it
     * @param payloadTenantId the tenant the entry is saved to, which is the declared one when there is one
     * @throws io.grpc.StatusRuntimeException with {@code PERMISSION_DENIED} when the worker may not write this payload
     */
    void checkSave(RequestOrResponseHeader header, String declaredTenantId, String payloadTenantId);

    @Singleton
    @Requires(missingBeans = WorkerTenantAccessGuard.class)
    @Secondary
    class Default implements WorkerTenantAccessGuard {

        @Override
        public <T> Partition<T> partition(RequestOrResponseHeader header, List<T> records, Function<T, String> tenantOf) {
            return new Partition<>(records, List.of());
        }

        @Override
        public void checkSave(RequestOrResponseHeader header, String declaredTenantId, String payloadTenantId) {
        }
    }

    record Partition<T>(List<T> allowed, List<T> denied) {
    }
}
