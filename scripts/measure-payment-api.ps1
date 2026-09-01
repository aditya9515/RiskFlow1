[CmdletBinding()]
param(
    [uri]$BaseUrl = "http://localhost:8080",
    [ValidateRange(1, 10000)]
    [int]$Requests = 200,
    [ValidateRange(1, 256)]
    [int]$Concurrency = 20,
    [ValidateSet("Unique", "IdenticalReplay")]
    [string]$Mode = "Unique",
    [ValidatePattern("^[A-Za-z0-9-]{1,80}$")]
    [string]$RunId = "benchmark-$([DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))",
    [ValidateRange(1, 300)]
    [int]$TimeoutSeconds = 10
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($Concurrency -gt $Requests) {
    throw "Concurrency must not exceed Requests."
}
if ($Mode -eq "IdenticalReplay" -and $Requests -lt 2) {
    throw "IdenticalReplay mode requires at least two requests."
}

function Get-NearestRankPercentile {
    param(
        [Parameter(Mandatory)]
        [double[]]$SortedValues,
        [Parameter(Mandatory)]
        [ValidateRange(0.01, 1.0)]
        [double]$Percentile
    )

    $index = [Math]::Ceiling($Percentile * $SortedValues.Count) - 1
    return $SortedValues[[Math]::Max(0, $index)]
}

$base = $BaseUrl.AbsoluteUri.TrimEnd("/")
$httpClient = [System.Net.Http.HttpClient]::new()
$httpClient.Timeout = [TimeSpan]::FromSeconds($TimeoutSeconds)

try {
    foreach ($path in @("healthz", "readyz")) {
        $preflight = $httpClient.GetAsync("$base/$path").GetAwaiter().GetResult()
        try {
            if (-not $preflight.IsSuccessStatusCode) {
                throw "Preflight /$path returned HTTP $([int]$preflight.StatusCode)."
            }
        }
        finally {
            $preflight.Dispose()
        }
    }

    $startedAt = [DateTimeOffset]::UtcNow
    $wallClock = [System.Diagnostics.Stopwatch]::StartNew()
    $results = 1..$Requests | ForEach-Object -Parallel {
        $index = $_
        $mode = $using:Mode
        $runId = $using:RunId
        $client = $using:httpClient
        $baseUrl = $using:base

        if ($mode -eq "IdenticalReplay") {
            $idempotencyKey = "$runId-identical"
            $customerId = "$runId-customer"
            $deviceId = "$runId-device"
            $amountMinor = 1250
        }
        else {
            $idempotencyKey = "$runId-$index"
            $customerId = "$runId-customer-$index"
            $deviceId = "$runId-device-$index"
            $amountMinor = 1000 + ($index % 1000)
        }

        $body = @{
            customer_id = $customerId
            merchant_id = "benchmark-merchant"
            device_id = $deviceId
            amount_minor = $amountMinor
            currency = "USD"
            country = "IN"
        } | ConvertTo-Json -Compress

        $request = [System.Net.Http.HttpRequestMessage]::new(
            [System.Net.Http.HttpMethod]::Post,
            "$baseUrl/v1/payments"
        )
        $request.Headers.TryAddWithoutValidation("Idempotency-Key", $idempotencyKey) | Out-Null
        $request.Content = [System.Net.Http.StringContent]::new(
            $body,
            [System.Text.Encoding]::UTF8,
            "application/json"
        )

        $timer = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            $response = $client.SendAsync($request).GetAwaiter().GetResult()
            try {
                $responseBody = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
                $timer.Stop()
                $paymentId = $null
                if ([int]$response.StatusCode -in 200, 201) {
                    $paymentId = ($responseBody | ConvertFrom-Json).id
                }
                [pscustomobject]@{
                    index = $index
                    status = [int]$response.StatusCode
                    elapsed_ms = $timer.Elapsed.TotalMilliseconds
                    payment_id = $paymentId
                    error = $null
                }
            }
            finally {
                $response.Dispose()
            }
        }
        catch {
            $timer.Stop()
            [pscustomobject]@{
                index = $index
                status = 0
                elapsed_ms = $timer.Elapsed.TotalMilliseconds
                payment_id = $null
                error = $_.Exception.Message
            }
        }
        finally {
            $request.Dispose()
        }
    } -ThrottleLimit $Concurrency
    $wallClock.Stop()
    $finishedAt = [DateTimeOffset]::UtcNow

    $failures = @($results | Where-Object { $_.error -or $_.status -notin 200, 201 })
    if ($failures.Count -gt 0) {
        $sample = $failures | Select-Object -First 3 | ConvertTo-Json -Compress
        throw "$($failures.Count) request(s) failed. Sample: $sample"
    }

    $statusCounts = [ordered]@{}
    foreach ($group in ($results | Group-Object status | Sort-Object Name)) {
        $statusCounts[[string]$group.Name] = $group.Count
    }
    $uniquePaymentIds = @($results.payment_id | Sort-Object -Unique).Count

    if ($Mode -eq "Unique") {
        if ($statusCounts["201"] -ne $Requests -or $uniquePaymentIds -ne $Requests) {
            throw "Unique mode expected $Requests HTTP 201 responses and payment IDs."
        }
    }
    else {
        if ($statusCounts["201"] -ne 1 -or $statusCounts["200"] -ne ($Requests - 1)) {
            throw "IdenticalReplay mode expected one HTTP 201 and $($Requests - 1) HTTP 200 responses."
        }
        if ($uniquePaymentIds -ne 1) {
            throw "IdenticalReplay mode returned $uniquePaymentIds payment IDs instead of one."
        }
    }

    [double[]]$latencies = @($results.elapsed_ms | Sort-Object)
    $summary = [ordered]@{
        schema_version = 1
        run_id = $RunId
        mode = $Mode
        base_url = $base
        started_at = $startedAt.ToString("O")
        finished_at = $finishedAt.ToString("O")
        request_count = $Requests
        concurrency = $Concurrency
        duration_seconds = [Math]::Round($wallClock.Elapsed.TotalSeconds, 6)
        requests_per_second = [Math]::Round($Requests / $wallClock.Elapsed.TotalSeconds, 3)
        latency_ms = [ordered]@{
            minimum = [Math]::Round($latencies[0], 3)
            mean = [Math]::Round(($latencies | Measure-Object -Average).Average, 3)
            p50 = [Math]::Round((Get-NearestRankPercentile $latencies 0.50), 3)
            p95 = [Math]::Round((Get-NearestRankPercentile $latencies 0.95), 3)
            p99 = [Math]::Round((Get-NearestRankPercentile $latencies 0.99), 3)
            maximum = [Math]::Round($latencies[-1], 3)
        }
        http_status_counts = $statusCounts
        unique_payment_ids = $uniquePaymentIds
        failed_requests = 0
    }
    $summary | ConvertTo-Json -Depth 4
}
finally {
    $httpClient.Dispose()
}
