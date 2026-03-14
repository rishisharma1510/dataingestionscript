<#
.SYNOPSIS
    Recursively renames files and folders whose full path exceeds the Windows limit.
.DESCRIPTION
    1. Trims trailing/leading spaces and redundant characters from names
    2. Removes conjunctions, prepositions, articles
    3. Abbreviates common words
    4. Truncates as a last resort
.PARAMETER TargetPath
    Root directory to scan. Defaults to current directory.
.PARAMETER MaxPathLength
    Maximum allowed full path length. Default 260 (Windows limit).
.PARAMETER DryRun
    Preview changes without renaming anything.
#>

param(
    [string]$TargetPath = (Get-Location).Path,
    [int]$MaxPathLength = 260,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

# --- Configuration ---

# Words to remove entirely (conjunctions, prepositions, articles)
$RemoveWords = @(
    'and', 'or', 'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for',
    'with', 'by', 'from', 'is', 'are', 'was', 'were', 'be', 'been',
    'but', 'not', 'this', 'that', 'it', 'as', 'if', 'its', 'has', 'have',
    'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should',
    'may', 'might', 'shall', 'can', 'into', 'over', 'under', 'between',
    'through', 'about', 'after', 'before', 'during', 'without', 'within',
    'along', 'across', 'behind', 'beyond', 'upon', 'toward', 'towards'
)

# Common abbreviations: full word -> short form
$Abbreviations = @{
    'information'   = 'info'
    'configuration' = 'config'
    'application'   = 'app'
    'document'      = 'doc'
    'documents'     = 'docs'
    'management'    = 'mgmt'
    'development'   = 'dev'
    'environment'   = 'env'
    'production'    = 'prod'
    'administration'= 'admin'
    'database'      = 'db'
    'specification' = 'spec'
    'specifications'= 'specs'
    'temporary'     = 'tmp'
    'directory'     = 'dir'
    'number'        = 'num'
    'maximum'       = 'max'
    'minimum'       = 'min'
    'average'       = 'avg'
    'password'      = 'pwd'
    'message'       = 'msg'
    'messages'      = 'msgs'
    'description'   = 'desc'
    'reference'     = 'ref'
    'organization'  = 'org'
    'department'    = 'dept'
    'company'       = 'co'
    'international' = 'intl'
    'version'       = 'ver'
    'release'       = 'rel'
    'resource'      = 'res'
    'resources'     = 'res'
    'service'       = 'svc'
    'services'      = 'svcs'
    'package'       = 'pkg'
    'library'       = 'lib'
    'function'      = 'func'
    'parameter'     = 'param'
    'parameters'    = 'params'
    'authentication'= 'auth'
    'authorization' = 'authz'
    'transaction'   = 'txn'
    'transactions'  = 'txns'
    'certificate'   = 'cert'
    'certificates'  = 'certs'
    'execution'     = 'exec'
    'operation'     = 'op'
    'operations'    = 'ops'
    'performance'   = 'perf'
    'communication' = 'comm'
    'connection'    = 'conn'
    'report'        = 'rpt'
    'summary'       = 'summ'
    'template'      = 'tmpl'
    'original'      = 'orig'
    'attachment'    = 'att'
    'requirements'  = 'reqs'
    'requirement'   = 'req'
    'implementation'= 'impl'
    'customer'      = 'cust'
    'account'       = 'acct'
    'government'    = 'govt'
    'financial'     = 'fin'
    'technical'     = 'tech'
    'supplement'    = 'supp'
    'response'      = 'resp'
    'notification'  = 'notif'
    'statistics'    = 'stats'
    'previous'      = 'prev'
    'general'       = 'gen'
    'standard'      = 'std'
    'schedule'      = 'sched'
    'analysis'      = 'anlys'
    'presentation'  = 'pres'
    'introduction'  = 'intro'
    'comparison'    = 'comp'
    'registration'  = 'reg'
    'distribution'  = 'dist'
    'instruction'   = 'instr'
    'instructions'  = 'instrs'
    'recommendation'= 'rec'
    'preliminary'   = 'prelim'
    'documentation' = 'docs'
    'preparation'   = 'prep'
    'screenshot'    = 'scrn'
    'attachment'    = 'att'
    'updated'       = 'upd'
    'quarterly'     = 'qtrly'
    'assessment'    = 'asmt'
    'inventory'     = 'inv'
    'manufacturing' = 'mfg'
    'engineering'   = 'eng'
    'professional'  = 'prof'
    'committee'     = 'cmte'
    'conference'    = 'conf'
    'national'      = 'natl'
    'association'   = 'assoc'
    'independent'   = 'indep'
    'management'    = 'mgmt'
}

# --- Helper Functions ---

function Get-ExcessLength {
    param([string]$FullPath)
    return ($FullPath.Length - $MaxPathLength)
}

function Remove-ExtraCharacters {
    param([string]$Name)
    # Remove multiple spaces/underscores/hyphens, trim edges
    $Name = $Name.Trim()
    $Name = $Name -replace '\s{2,}', ' '
    $Name = $Name -replace '_{2,}', '_'
    $Name = $Name -replace '-{2,}', '-'
    $Name = $Name -replace '\s*[-_]\s*', '_'  # normalize separators
    # Remove special characters that aren't meaningful
    $Name = $Name -replace '[~#%&{}\\<>!@`\$\+\=\|]', ''
    return $Name
}

function Remove-Conjunctions {
    param([string]$Name)
    $separator = if ($Name -match '_') { '_' } elseif ($Name -match ' ') { ' ' } elseif ($Name -match '-') { '-' } else { $null }

    if ($null -eq $separator) { return $Name }

    $parts = $Name -split [regex]::Escape($separator)
    $filtered = $parts | Where-Object { $_.ToLower() -notin $RemoveWords }

    if ($filtered.Count -eq 0) { return $Name }
    return ($filtered -join $separator)
}

function Apply-Abbreviations {
    param([string]$Name)
    $separator = if ($Name -match '_') { '_' } elseif ($Name -match ' ') { ' ' } elseif ($Name -match '-') { '-' } else { $null }

    if ($null -eq $separator) {
        # No separator - try matching the whole name (minus extension concern handled by caller)
        $lower = $Name.ToLower()
        if ($Abbreviations.ContainsKey($lower)) {
            return $Abbreviations[$lower]
        }
        return $Name
    }

    $parts = $Name -split [regex]::Escape($separator)
    $abbreviated = $parts | ForEach-Object {
        $lower = $_.ToLower()
        if ($Abbreviations.ContainsKey($lower)) {
            $Abbreviations[$lower]
        } else {
            $_
        }
    }
    return ($abbreviated -join $separator)
}

function Remove-Vowels {
    # Remove vowels from words longer than 3 chars, keeping first letter
    param([string]$Name)
    $separator = if ($Name -match '_') { '_' } elseif ($Name -match ' ') { ' ' } elseif ($Name -match '-') { '-' } else { $null }

    if ($null -eq $separator) {
        if ($Name.Length -gt 3) {
            return $Name[0] + (($Name.Substring(1)) -replace '[aeiouAEIOU]', '')
        }
        return $Name
    }

    $parts = $Name -split [regex]::Escape($separator)
    $result = $parts | ForEach-Object {
        if ($_.Length -gt 3) {
            $_[0] + (($_.Substring(1)) -replace '[aeiouAEIOU]', '')
        } else {
            $_
        }
    }
    return ($result -join $separator)
}

function Truncate-Name {
    param(
        [string]$Name,
        [int]$MaxLength
    )
    if ($Name.Length -le $MaxLength -or $MaxLength -lt 1) { return $Name }
    return $Name.Substring(0, $MaxLength)
}

function Shorten-FileName {
    param(
        [string]$Directory,
        [string]$FileName,
        [int]$Excess
    )

    $extension = [System.IO.Path]::GetExtension($FileName)
    $baseName  = [System.IO.Path]::GetFileNameWithoutExtension($FileName)
    $original  = $baseName

    # Step 1: Clean up extra characters
    $baseName = Remove-ExtraCharacters $baseName

    # Step 2: Remove conjunctions/prepositions
    if (($Directory.Length + 1 + $baseName.Length + $extension.Length) -gt $MaxPathLength) {
        $baseName = Remove-Conjunctions $baseName
    }

    # Step 3: Apply abbreviations
    if (($Directory.Length + 1 + $baseName.Length + $extension.Length) -gt $MaxPathLength) {
        $baseName = Apply-Abbreviations $baseName
    }

    # Step 4: Remove vowels (except first letter of each word)
    if (($Directory.Length + 1 + $baseName.Length + $extension.Length) -gt $MaxPathLength) {
        $baseName = Remove-Vowels $baseName
    }

    # Step 5: Hard truncate as last resort
    $maxBase = $MaxPathLength - $Directory.Length - 1 - $extension.Length
    if ($maxBase -lt 4) { $maxBase = 4 }
    if ($baseName.Length -gt $maxBase) {
        $baseName = Truncate-Name -Name $baseName -MaxLength $maxBase
    }

    return "$baseName$extension"
}

function Shorten-FolderName {
    param(
        [string]$ParentPath,
        [string]$FolderName
    )

    # Step 1: Clean up extra characters
    $FolderName = Remove-ExtraCharacters $FolderName

    # Step 2: Remove conjunctions (only if still over limit)
    if (($ParentPath.Length + 1 + $FolderName.Length) -gt $MaxPathLength) {
        $FolderName = Remove-Conjunctions $FolderName
    }

    # Step 3: Apply abbreviations (only if still over limit)
    if (($ParentPath.Length + 1 + $FolderName.Length) -gt $MaxPathLength) {
        $FolderName = Apply-Abbreviations $FolderName
    }

    # Step 4: Remove vowels (only if still over limit)
    if (($ParentPath.Length + 1 + $FolderName.Length) -gt $MaxPathLength) {
        $FolderName = Remove-Vowels $FolderName
    }

    # Step 5: Hard truncate as last resort
    $maxFolder = $MaxPathLength - $ParentPath.Length - 1
    if ($maxFolder -lt 4) { $maxFolder = 4 }
    if (($ParentPath.Length + 1 + $FolderName.Length) -gt $MaxPathLength) {
        $FolderName = Truncate-Name -Name $FolderName -MaxLength $maxFolder
    }

    return $FolderName
}

# --- Main Logic ---

Write-Host ""
Write-Host "=== Long Path Renamer ===" -ForegroundColor Cyan
Write-Host "Target:    $TargetPath"
Write-Host "Max Path:  $MaxPathLength"
Write-Host "Mode:      $(if ($DryRun) { 'DRY RUN (no changes)' } else { 'LIVE' })"
Write-Host ""

# Track renames to update paths for subsequent items
$renameLog = @()

# Process FOLDERS first (deepest first so child renames don't break parent paths)
Write-Host "--- Processing Folders (deepest first) ---" -ForegroundColor Yellow
$folders = Get-ChildItem -Path $TargetPath -Recurse -Directory -ErrorAction SilentlyContinue |
    Sort-Object { $_.FullName.Length } -Descending

foreach ($folder in $folders) {
    $currentPath = $folder.FullName

    # Update path if a parent was already renamed
    foreach ($entry in $renameLog) {
        if ($currentPath.StartsWith($entry.OldPath + [IO.Path]::DirectorySeparatorChar) -or $currentPath -eq $entry.OldPath) {
            $currentPath = $currentPath.Replace($entry.OldPath, $entry.NewPath)
        }
    }

    if ($currentPath.Length -le $MaxPathLength) { continue }

    $parentPath = Split-Path $currentPath -Parent
    $folderName = Split-Path $currentPath -Leaf
    $newName    = Shorten-FolderName -ParentPath $parentPath -FolderName $folderName
    $newPath    = Join-Path $parentPath $newName

    if ($newName -eq $folderName) { continue }

    # Avoid collisions
    $counter = 1
    $candidate = $newPath
    while (Test-Path $candidate) {
        $candidate = "${newPath}_$counter"
        $counter++
    }
    $newPath = $candidate

    Write-Host "[FOLDER] " -ForegroundColor Magenta -NoNewline
    Write-Host "$currentPath" -ForegroundColor Red
    Write-Host "      -> $newPath" -ForegroundColor Green

    if (-not $DryRun) {
        Rename-Item -LiteralPath $currentPath -NewName (Split-Path $newPath -Leaf) -Force
    }

    $renameLog += @{ OldPath = $currentPath; NewPath = $newPath }
}

# Process FILES (deepest first)
Write-Host ""
Write-Host "--- Processing Files ---" -ForegroundColor Yellow
$files = Get-ChildItem -Path $TargetPath -Recurse -File -ErrorAction SilentlyContinue |
    Sort-Object { $_.FullName.Length } -Descending

$changedCount = 0
$skippedCount = 0

foreach ($file in $files) {
    $currentPath = $file.FullName

    # Update path if a parent folder was renamed
    foreach ($entry in $renameLog) {
        if ($currentPath.StartsWith($entry.OldPath + [IO.Path]::DirectorySeparatorChar)) {
            $currentPath = $currentPath.Replace($entry.OldPath, $entry.NewPath)
        }
    }

    if ($currentPath.Length -le $MaxPathLength) {
        $skippedCount++
        continue
    }

    $directory = Split-Path $currentPath -Parent
    $fileName  = Split-Path $currentPath -Leaf
    $excess    = Get-ExcessLength -FullPath $currentPath

    $newName = Shorten-FileName -Directory $directory -FileName $fileName -Excess $excess
    $newPath = Join-Path $directory $newName

    if ($newName -eq $fileName) {
        Write-Host "[SKIP]  Cannot shorten further: $currentPath" -ForegroundColor DarkYellow
        continue
    }

    # Avoid collisions
    $counter = 1
    $candidate = $newPath
    $baseNewName = [System.IO.Path]::GetFileNameWithoutExtension($newName)
    $ext = [System.IO.Path]::GetExtension($newName)
    while (Test-Path $candidate) {
        $candidate = Join-Path $directory "${baseNewName}_$counter$ext"
        $counter++
    }
    $newPath = $candidate
    $newName = Split-Path $newPath -Leaf

    $pathLen = $newPath.Length
    $status = if ($pathLen -le $MaxPathLength) { "OK" } else { "STILL LONG ($pathLen)" }

    Write-Host "[FILE]  " -ForegroundColor Magenta -NoNewline
    Write-Host "$currentPath" -ForegroundColor Red
    Write-Host "     -> $newPath [$status]" -ForegroundColor Green

    if (-not $DryRun) {
        Rename-Item -LiteralPath $currentPath -NewName $newName -Force
    }

    $renameLog += @{ OldPath = $currentPath; NewPath = $newPath }
    $changedCount++
}

Write-Host ""
Write-Host "=== Summary ===" -ForegroundColor Cyan
Write-Host "Files renamed:  $changedCount"
Write-Host "Files skipped:  $skippedCount (already within limit)"
Write-Host "Folders renamed: $($renameLog.Count - $changedCount)"
if ($DryRun) {
    Write-Host ""
    Write-Host "This was a DRY RUN. No files were changed." -ForegroundColor Yellow
    Write-Host "Remove -DryRun to apply changes." -ForegroundColor Yellow
}
Write-Host ""
