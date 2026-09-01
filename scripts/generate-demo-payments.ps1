[CmdletBinding()]
param(
    [uri]$BaseUrl = "http://localhost:8080",
    [ValidatePattern("^[A-Za-z0-9-]{1,80}$")]
    [string]$RunId = "demo-$([DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmss'))",
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

$scenarios = @(
    [pscustomobject]@{
        name = "allow"
        amount_minor = 1250
        expected_status = "ALLOWED"
    },
    [pscustomobject]@{
        name = "review"
        amount_minor = 100000
        expected_status = "REVIEW"
    },
    [pscustomobject]@{
        name = "block"
        amount_minor = 500000
        expected_status = "BLOCKED"
    }
)

function Get-ResponseBody {
    param(
        [Parameter(Mandatory)]
        [System.Net.Http.HttpResponseMessage]$Response
    )

    return $Response.Content.ReadAsStringAsync().GetAwaiter().GetResult()
}

try {
    foreach ($path in @("healthz", "readyz")) {
        $preflight = $client.GetAsync("$base/$path").GetAwaiter().GetResult()
        try {
            if (-not $preflight.IsSuccessStatusCode) {
                $content = Get-ResponseBody -Response $preflight
                throw "Preflight /$path returned HTTP $([int]$preflight.StatusCode): $content"
            }
        }
        finally {
            $preflight.Dispose()
        }
    }

    $startedAt = [DateTimeOffset]::UtcNow
    $created = foreach ($scenario in $scenarios) {
        $body = @{
            customer_id = "$RunId-$($scenario.name)-customer"
            merchant_id = "demo-merchant"
            device_id = "$RunId-$($scenario.name)-device"
            amount_minor = $scenario.amount_minor
            currency = "USD"
            country = "IN"
        } | ConvertTo-Json -Compress

        $request = [System.Net.Http.HttpRequestMessage]::new(
            [System.Net.Http.HttpMethod]::Post,
            "$base/v1/payments"
        )
        $request.Headers.TryAddWithoutValidation(
            "Idempotency-Key",
            "$RunId-$($scenario.name)"
        ) | Out-Null
        $request.Content = [System.Net.Http.StringContent]::new(
            $body,
            [System.Text.Encoding]::UTF8,
            "application/json"
        )

        try {
            $response = $client.SendAsync($request).GetAwaiter().GetResult()
            try {
                $content = Get-ResponseBody -Response $response
                if ([int]$response.StatusCode -notin 200, 201) {
                    throw "Scenario $($scenario.name) returned HTTP $([int]$response.StatusCode): $content"
                }
                $payment = $content | ConvertFrom-Json
                [pscustomobject]@{
                    scenario = $scenario.name
                    expected_status = $scenario.expected_status
                    create_status = [int]$response.StatusCode
                    replayed = [int]$response.StatusCode -eq 200
                    payment = $payment
                }
            }
            finally {
                $response.Dispose()
            }
        }
        finally {
            $request.Dispose()
        }
    }

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    $final = foreach ($item in $created) {
        $payment = $item.payment
        $polls = 0

        while ($payment.status -eq "PENDING_RISK" -and [DateTimeOffset]::UtcNow -lt $deadline) {
            Start-Sleep -Milliseconds $PollIntervalMilliseconds
            $polls++

            $response = $client.GetAsync("$base/v1/payments/$($payment.id)").GetAwaiter().GetResult()
            try {
                $content = Get-ResponseBody -Response $response
                if (-not $response.IsSuccessStatusCode) {
                    throw "Retrieving payment $($payment.id) returned HTTP $([int]$response.StatusCode): $content"
                }
                $payment = $content | ConvertFrom-Json
            }
            finally {
                $response.Dispose()
            }
        }

        if ($payment.status -eq "PENDING_RISK") {
            throw "Scenario $($item.scenario) remained PENDING_RISK after $TimeoutSeconds seconds."
        }
        if ($payment.status -ne $item.expected_status) {
            throw "Scenario $($item.scenario) expected $($item.expected_status) but reached $($payment.status). Verify the default risk thresholds and pinned model artifact."
        }

        [pscustomobject]@{
            scenario = $item.scenario
            payment_id = $payment.id
            amount_minor = $payment.amount_minor
            create_status = $item.create_status
            replayed = $item.replayed
            initial_status = $item.payment.status
            final_status = $payment.status
            polls = $polls
        }
    }

    [ordered]@{
        schema_version = 1
        run_id = $RunId
        started_at = $startedAt.ToString("O")
        finished_at = [DateTimeOffset]::UtcNow.ToString("O")
        payments_created = @($final | Where-Object { -not $_.replayed }).Count
        payments_replayed = @($final | Where-Object replayed).Count
        outcomes = [ordered]@{
            allowed = @($final | Where-Object final_status -eq "ALLOWED").Count
            review = @($final | Where-Object final_status -eq "REVIEW").Count
            blocked = @($final | Where-Object final_status -eq "BLOCKED").Count
        }
        payments = @($final)
    } | ConvertTo-Json -Depth 5
}
finally {
    $client.Dispose()
}
