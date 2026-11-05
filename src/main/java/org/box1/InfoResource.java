package org.box1;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.openapi.annotations.Operation;

@Path("/api/info")
@Produces(MediaType.APPLICATION_JSON)
public class InfoResource {
    private static final String GIT_REF = readGitRef();

    @ConfigProperty(name = "quarkus.application.version")
    String version;

    @GET
    @Operation(summary = "Return application, Java runtime, and source revision information")
    public ApplicationInfo info() {
        return new ApplicationInfo("inventory-service", version, Runtime.version().feature(), "tekton-gitops", GIT_REF);
    }

    private static String readGitRef() {
        try (InputStream input = InfoResource.class.getResourceAsStream("/META-INF/box1-build.properties")) {
            if (input == null) {
                return "unknown";
            }
            Properties metadata = new Properties();
            metadata.load(input);
            return metadata.getProperty("git.ref", "unknown");
        } catch (IOException error) {
            throw new IllegalStateException("Unable to read application build metadata", error);
        }
    }

    public record ApplicationInfo(String name, String version, int javaVersion, String deliveryMode, String gitRef) {}
}
