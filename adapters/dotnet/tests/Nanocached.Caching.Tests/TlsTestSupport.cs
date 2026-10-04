using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Nanocached.Caching.Tests;

/// <summary>Throwaway self-signed certificate for the TLS options' tests —
/// the same BCL-only approach sdk/dotnet/tests/Nanocached.Tests uses (no
/// openssl on PATH, no extra test dependency). The certificate is its own
/// trust anchor, like a real private CA's root passed as
/// <see cref="NanocachedCacheOptions.Ca"/>.</summary>
internal static class TlsTestSupport
{
    /// <summary>A self-signed certificate whose only Subject Alternative
    /// Name is the loopback IP the tests dial.</summary>
    internal static X509Certificate2 GenerateLoopbackCertificate()
    {
        using RSA rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=127.0.0.1", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var sanBuilder = new SubjectAlternativeNameBuilder();
        sanBuilder.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(sanBuilder.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        X509Certificate2 cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(3650));
        // Round-tripped through PFX: a fresh CreateSelfSigned key is
        // ephemeral, which SslStream's server side can't reliably use.
        return X509CertificateLoader.LoadPkcs12(
            cert.Export(X509ContentType.Pfx), password: null, X509KeyStorageFlags.Exportable);
    }

    /// <summary>Writes just the public certificate as a PEM file — the
    /// shape <see cref="NanocachedCacheOptions.Ca"/> expects.</summary>
    internal static string WritePemCertificate(X509Certificate2 certificate)
    {
        string path = Path.Combine(Path.GetTempPath(), $"nanocached-adapter-test-ca-{Guid.NewGuid():N}.pem");
        File.WriteAllText(path, certificate.ExportCertificatePem());
        return path;
    }
}
