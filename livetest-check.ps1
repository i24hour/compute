function Test-Url($url, $timeoutSec) {
  $sw = [System.Diagnostics.Stopwatch]::StartNew()
  try {
    $r = Invoke-WebRequest $url -UseBasicParsing -TimeoutSec $timeoutSec
    $sw.Stop()
    Write-Output "URL=$url"
    Write-Output "StatusCode=$($r.StatusCode)"
    Write-Output "Len=$($r.Content.Length)"
    Write-Output "Seconds=$([math]::Round($sw.Elapsed.TotalSeconds, 2))"
    Write-Output "Error="
  } catch {
    $sw.Stop()
    $code = $null
    if ($_.Exception.Response) {
      try { $code = [int]$_.Exception.Response.StatusCode } catch {}
    }
    Write-Output "URL=$url"
    Write-Output "StatusCode=$code"
    Write-Output "Len="
    Write-Output "Seconds=$([math]::Round($sw.Elapsed.TotalSeconds, 2))"
    Write-Output "Error=$($_.Exception.Message)"
  }
  Write-Output "---"
}

$p = Get-Process -Id 226376 -ErrorAction SilentlyContinue
if ($p) { Write-Output "PID226376=running name=$($p.ProcessName)" } else { Write-Output "PID226376=not_running" }

Write-Output "===TEST1==="
Test-Url 'http://127.0.0.1:5050/livetest' 10

Write-Output "===TEST2==="
$result2 = Test-Url 'http://127.0.0.1:5050/api/livetest' 180
$test2Seconds = ($result2 | Where-Object { $_ -match '^Seconds=' }) -replace 'Seconds=',''
$test2Error = ($result2 | Where-Object { $_ -match '^Error=' }) -replace 'Error=',''

if ($test2Error -or ([double]$test2Seconds -ge 60)) {
  Write-Output "===TEST3==="
  Test-Url 'http://127.0.0.1:5050/api/livetest?tail=15000' 60
}
