[CmdletBinding()]
param(
    [uri]$BaseUrl = "http://localhost:8080",
    [ValidatePattern("^[A-Za-z0-9-]{1,80}$")]
    [string]$RunId = "e2e-$([DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))",
    [ValidateRange(1, 300)]
    [int]$TimeoutSeconds = 30,
    [ValidateRange(10, 5000)]
    [int]$PollIntervalMilliseconds = 100
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$base = $BaseUrl.AbsoluteUri.TrimEnd("/")
$client = [System.Net.Http.HttpClient]::new()
$client.Timeout = [TimeSpan]::FromSeconds(5)

try {
    foreach ($path in @("healthz", "readyz")) {
        $preflight = $client.GetAsync("$base/$path").GetAwaiter().GetResult()
        try {
            if (-not $preflight.IsSuccessStatusCode) {
                throw "Preflight /$path returned HTTP $([int]$preflight.StatusCode)."
            }
        }
        finally {
            $preflight.Dispose()
        }
    }

    $body = @{
        customer_id = "$RunId-customer"
        merchant_id = "e2e-merchant"
        device_id = "$RunId-device"
        amount_minor = 1250
        currency = "USD"
        country = "IN"
    } | ConvertTo-Json -Compress
    $request = [System.Net.Http.HttpRequestMessage]::new(
        [System.Net.Http.HttpMethod]::Post,
        "$base/v1/payments"
    )
    $request.Headers.TryAddWithoutValidation("Idempotency-Key", "$RunId-payment") | Out-Null
    $request.Content = [System.Net.Http.StringContent]::new(
        $body,
        [System.Text.Encoding]::UTF8,
        "application/json"
    )

    $timer = [System.Diagnostics.Stopwatch]::StartNew()
    try {
        $createResponse = $client.SendAsync($request).GetAwaiter().GetResult()
        try {
            $createBody = $createResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            if ([int]$createResponse.StatusCode -ne 201) {
                throw "Payment creation returned HTTP $([int]$createResponse.StatusCode): $createBody"
            }
            $createdPayment = $createBody | ConvertFrom-Json
        }
        finally {
            $createResponse.Dispose()
        }
    }
    finally {
        $request.Dispose()
    }

    $polls = 0
    $finalPayment = $createdPayment
    while ($finalPayment.status -eq "PENDING_RISK" -and $timer.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
        Start-Sleep -Milliseconds $PollIntervalMilliseconds
        $polls++
        $getResponse = $client.GetAsync("$base/v1/payments/$($createdPayment.id)").GetAwaiter().GetResult()
        try {
            $getBody = $getResponse.Content.ReadAsStringAsync().GetAwaiter().GetResult()
            if (-not $getResponse.IsSuccessStatusCode) {
                throw "Payment retrieval returned HTTP $([int]$getResponse.StatusCode): $getBody"
            }
            $finalPayment = $getBody | ConvertFrom-Json
        }
        finally {
            $getResponse.Dispose()
        }
    }
    $timer.Stop()

    if ($finalPayment.status -eq "PENDING_RISK") {
        throw "Payment $($createdPayment.id) remained PENDING_RISK after $TimeoutSeconds seconds."
    }
    if ($finalPayment.status -notin "ALLOWED", "REVIEW", "BLOCKED") {
        throw "Payment ended in unexpected status $($finalPayment.status)."
    }

    [ordered]@{
        schema_version = 1
        run_id = $RunId
        payment_id = $createdPayment.id
        initial_status = $createdPayment.status
        final_status = $finalPayment.status
        observed_end_to_end_ms = [Math]::Round($timer.Elapsed.TotalMilliseconds, 3)
        poll_interval_ms = $PollIntervalMilliseconds
        polls = $polls
    } | ConvertTo-Json
}
finally {
    $client.Dispose()
}
