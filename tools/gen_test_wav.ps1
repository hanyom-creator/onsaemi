<#
S1 STT 종료 판정 확인용 테스트 음성을 생성한다 (WBS 3.1.2).
System.Speech 로 한국어 문장을 16kHz mono 16bit WAV 로 합성한다.

문장은 스크립트에 직접 넣지 않고 UTF-8 텍스트 파일에서 읽는다. Windows
PowerShell 5.1은 BOM이 없는 .ps1 파일을 시스템 코드페이지(한국어 Windows면
CP949)로 읽어서, 소스에 직접 적은 한글 리터럴이 파싱 단계에서 깨지는
문제가 있었다 — 그래서 문장은 별도 파일로 분리하고, 이 스크립트 자체는
UTF-8 BOM으로 저장한다.

사용:
    powershell -File tools/gen_test_wav.ps1 samples/test_ko.txt samples/test_ko.wav
#>
param(
    [Parameter(Mandatory = $true)]
    [string]$TextPath,

    [Parameter(Mandatory = $true)]
    [string]$OutPath
)

if (-not (Test-Path $TextPath)) {
    Write-Error "텍스트 파일이 없다: $TextPath"
    exit 1
}

$text = Get-Content -Raw -Encoding UTF8 -Path $TextPath

Add-Type -AssemblyName System.Speech
$synth = New-Object System.Speech.Synthesis.SpeechSynthesizer

$koVoice = $synth.GetInstalledVoices() |
    Where-Object { $_.VoiceInfo.Culture.Name -eq 'ko-KR' } |
    Select-Object -First 1

if ($koVoice) {
    $synth.SelectVoice($koVoice.VoiceInfo.Name)
    Write-Output "한국어 음성 선택: $($koVoice.VoiceInfo.Name)"
} else {
    Write-Warning "한국어 음성이 설치되어 있지 않다. 기본 음성으로 진행한다."
}

$outDir = Split-Path -Parent $OutPath
if ($outDir -and -not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Path $outDir | Out-Null
}

$format = New-Object System.Speech.AudioFormat.SpeechAudioFormatInfo(
    16000,
    [System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen,
    [System.Speech.AudioFormat.AudioChannel]::Mono
)
$synth.SetOutputToWaveFile($OutPath, $format)
$synth.Speak($text)
$synth.Dispose()

Write-Output "생성 완료: $OutPath"
