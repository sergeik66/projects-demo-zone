param (
    [Parameter(Mandatory=$true)]
    [string]$ReleaseVersion,
   
    [Parameter(Mandatory=$false)]
    [string]$SourceBranch = "main",
   
    [Parameter(Mandatory=$true)]
    [string]$Organization,
   
    [Parameter(Mandatory=$true)]
    [string]$Project,
   
    [Parameter(Mandatory=$true)]
    [string]$Repository,
   
    [Parameter(Mandatory=$false)]
    [string]$ProjectPrefix = "data-product",
   
    [Parameter(Mandatory=$false)]
    [int]$DaysToLookBack = 8,

    # Teams notification parameters
    [Parameter(Mandatory=$false)]
    [string]$TeamsChannelWebUrl,
   
    # Control whether to send Teams notification
    [Parameter(Mandatory=$false)]
    [bool]$SendTeamsNotification = $false    
)

# Service Principal Authentication details will come from environment variables:
# ARM_CLIENT_ID
# ARM_CLIENT_SECRET
# ARM_TENANT_ID

function Get-DevOpsAuthToken {
    try {
        $resource = "499b84ac-1321-427f-aa17-267ca6975798"
        $authUrl = "https://login.microsoftonline.com/$env:ARM_TENANT_ID/oauth2/token"

        # Construct token request
        $body = @{
            grant_type    = "client_credentials"
            client_id     = $env:ARM_CLIENT_ID
            client_secret = $env:ARM_CLIENT_SECRET
            resource      = $resource
        }

        # Get token
        $response = Invoke-RestMethod -Method Post -Uri $authUrl -Body $body
        $token = $response.access_token

        return $token
    }
    catch {
        Write-Error "Failed to get Azure DevOps token: $_"
        throw
    }
}

function Test-GitRepository {
  $gitDir = Get-Command "git" -ErrorAction SilentlyContinue
  if (-not $gitDir) {
      throw "Git is not installed."
  }
}

function Get-ProjectId {
    param (
        [string]$Organization,
        [string]$ProjectName,
        [string]$Token
    )
   
    Write-Host "##[debug]Looking up Project ID for: $ProjectName"
   
    # Construct API URL to get project details
    $uri = "https://dev.azure.com/$Organization/_apis/projects?api-version=7.0"
    Write-Host "##[debug]API URL: $uri"
   
    $response = Invoke-AzDevOpsApi -Uri $uri -Token $Token

    # filter response by project name
    $response = $response.value | Where-Object { $_.name -eq $ProjectName }
   
    if ($response.count -eq 0) {
        throw "Project '$ProjectName' not found in organization '$Organization'"
    }
   
    Write-Host "##[debug]Found project response: $($response | ConvertTo-Json -Depth 3)"
    $projectId = $response.id
    Write-Host "##[debug]Found Project ID: $projectId"
   
    return $projectId
}

function Get-RepositoryId {
    param (
        [string]$Organization,
        [string]$ProjectId,
        [string]$RepositoryName,
        [string]$Token
    )
   
    Write-Host "##[debug]Looking up Repository ID for: $RepositoryName in Project ID: $ProjectId"
   
    # Construct API URL to get repository details
    $uri = "https://dev.azure.com/$Organization/$ProjectId/_apis/git/repositories?api-version=7.0"
   
    $response = Invoke-AzDevOpsApi -Uri $uri -Token $Token
   
    if ($response.count -eq 0) {
        throw "No repositories found in project ID '$ProjectId'"
    }
   
    $repository = $response.value | Where-Object { $_.name -eq $RepositoryName }
   
    if (-not $repository) {
        throw "Repository '$RepositoryName' not found in project ID '$ProjectId'"
    }
   
    $repositoryId = $repository.id
    Write-Host "##[debug]Found Repository ID: $repositoryId"
   
    return $repositoryId
}

function Initialize-GitConfiguration {
     param (
        [string]$Token,
        [string]$Organization,
        [string]$Project,
        [string]$Repository
    )    
    Write-Host "##[debug]Initializing Git configurations..."

    # Configure Git to avoid common issues
    git config --global core.longpaths true
    git config --global core.autocrlf false
    git config --global core.packedGitLimit 512m
    git config --global core.packedGitWindowSize 512m
    git config --global pack.windowMemory 512m
    git config --global pack.packSizeLimit 512m
    git config --global http.postBuffer 524288000

    # Set git config for commits
    git config --global user.email "azure-pipeline@bhg.com"
    git config --global user.name "Azure Pipeline"
}

function New-ReleaseBranch {
    param (
        [string]$Version,
        [string]$SourceBranch,
        [string]$Organization,
        [string]$ProjectId,
        [string]$RepositoryId
    )

    Write-Host "##[section]Creating release branch for version $Version from $SourceBranch"
    try {
        $tenantId = $env:ARM_TENANT_ID
        $clientId = $env:ARM_CLIENT_ID
        $clientSecret = $env:ARM_CLIENT_SECRET

        # Get Azure AD token
        $tokenUrl = "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
        $bodyParams = @{
            grant_type    = "client_credentials"
            client_id     = $clientId
            client_secret = $clientSecret
            scope         = "499b84ac-1321-427f-aa17-267ca6975798/.default"
        }

        Add-Type -AssemblyName System.Web
        $encodedBody = ($bodyParams.GetEnumerator() | ForEach-Object {
            "$($_.Key)=$([System.Web.HttpUtility]::UrlEncode($_.Value))"
        }) -join "&"

        $authResult = Invoke-RestMethod -Method Post -Uri $tokenUrl `
            -ContentType "application/x-www-form-urlencoded" `
            -Body $encodedBody

        # Create a temp directory for repo operations
        $tempDir = Join-Path $env:TEMP "release_$(Get-Random)"
        New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
        Set-Location $tempDir
       
        Write-Host "##[debug]Working in temporary directory: $tempDir"

        # Clone repository with source branch
        $repoUrl = "https://oauth2:$($authResult.access_token)@dev.azure.com/$Organization/$ProjectId/_git/$RepositoryId"
        Write-Host "##[debug]Cloning repository from $SourceBranch branch..."
        git clone -b $SourceBranch --single-branch --depth=1 $repoUrl .
        if ($LASTEXITCODE -ne 0) { throw "Failed to clone repository" }
       
        # Create and switch to new branch
        $branchName = $ReleaseVersion
        Write-Host "##[debug]Creating branch: $branchName"
        git checkout -b $branchName
        if ($LASTEXITCODE -ne 0) { throw "Failed to create branch $branchName" }
       
        Write-Host "##[debug]Successfully created release branch: $branchName"
        return @{
            BranchName = $branchName
            TempDir = $tempDir
        }
    }
    catch {
        Write-Error "Failed to create release branch: $_"
        throw
    }
}

function Invoke-AzDevOpsApi {
    param (
        [string]$Uri,
        [string]$Token,
        [string]$Method = "GET",
        [object]$Body = $null,
        [string]$ContentType = "application/json"
    )
   
    $headers = @{
        "Authorization" = "Bearer $Token"
        "Accept" = "application/json"
    }
   
    $params = @{
        Uri = $Uri
        Headers = $headers
        Method = $Method
        ContentType = $ContentType
        UseBasicParsing = $true
    }
   
    if ($Body -and $Method -ne "GET") {
        $params.Body = if ($Body -is [string]) { $Body } else { $Body | ConvertTo-Json -Depth 100 }
    }
   
    # Invoke the REST API call
    Write-Host "##[debug]Invoking API: $Uri"
    Write-Host "##[debug]Method: $Method"
   
    try {
        if ($Body -eq $null) {
            $response = Invoke-RestMethod -Uri $Uri -Method $Method -Headers $headers
        }
        else {
            $response = Invoke-RestMethod -Uri $Uri -Method $Method -Headers $headers -Body $($params.Body)
        }
       
        return $response
    }
    catch {
        Write-Host "##[error]API call failed: $_"
        Write-Host "##[error]Status Code: $($_.Exception.Response.StatusCode.value__)"
       
        if ($_.ErrorDetails.Message) {
            Write-Host "##[error]Error Details: $($_.ErrorDetails.Message)"
        }
       
        # Return null instead of throwing to allow the script to continue
        return $null
    }
}

function Get-CompletedPullRequests {
    param (
        [string]$Organization,
        [string]$ProjectId,
        [string]$RepositoryId,
        [int]$DaysToLookBack,
        [string]$Token
    )
   
    $endDate = Get-Date
    $startDate = $endDate.AddDays(-$DaysToLookBack)
   
    Write-Host "##[debug]Getting completed pull requests between $($startDate.ToString('yyyy-MM-dd')) and $($endDate.ToString('yyyy-MM-dd'))"
   
    # Format dates for API
    $fromDate = $startDate.ToString("yyyy-MM-dd")
    $toDate = $endDate.ToString("yyyy-MM-dd")
   
    # Construct API URL to get completed pull requests
    $uri = "https://dev.azure.com/$Organization/$ProjectId/_apis/git/repositories/$RepositoryId/pullrequests?searchCriteria.status=completed&searchCriteria.minTime=$fromDate&searchCriteria.targetRefName=refs/heads/main&api-version=7.1"
   
    $pullRequests = Invoke-AzDevOpsApi -Uri $uri -Token $Token
   
    if ($pullRequests -and $pullRequests.value) {
        # Filter pull requests by completion date
        $filteredPRs = $pullRequests.value | Where-Object {
            (Get-Date $_.closedDate) -ge $startDate -and (Get-Date $_.closedDate) -le $endDate
        }
       
        Write-Host "##[debug]Found $($filteredPRs.Count) completed pull requests in date range"
        return $filteredPRs
    }
   
    Write-Host "##[debug]No pull requests found or API call failed"
    return @()
}

function Get-WorkItemsForPullRequest {
    param (
        [string]$Organization,
        [string]$ProjectId,
        [string]$RepositoryId,
        [int]$PullRequestId,
        [string]$Token
    )
   
    # Construct API URL to get work items for pull request
    $uri = "https://dev.azure.com/$Organization/$ProjectId/_apis/git/repositories/$RepositoryId/pullRequests/$PullRequestId/workitems?api-version=7.1"
   
    $workItems = Invoke-AzDevOpsApi -Uri $uri -Token $Token
   
    if ($workItems -and $workItems.value) {
        Write-Host "##[debug]Found $($workItems.value.Count) work items for PR #$PullRequestId"
        return $workItems.value
    }
   
    Write-Host "##[debug]No work items found for PR #$PullRequestId or API call failed"
    return @()
}

function Get-WorkItemDetails {
    param (
        [string]$Organization,
        [string]$ProjectId,
        [int]$WorkItemId,
        [string]$Token
    )
   
    # Construct API URL to get work item details
    $uri = "https://dev.azure.com/$Organization/$ProjectId/_apis/wit/workitems/$WorkItemId`?`$expand=all&api-version=7.1"
   
    $workItem = Invoke-AzDevOpsApi -Uri $uri -Token $Token
   
    return $workItem
}

function Format-ReleaseNotes {
    param (
        [PSCustomObject]$PullRequestInfo,
        [PSCustomObject]$WorkItemInfo
    )
   
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
   
    # Extract fields from work item 
    $workItemType = $WorkItemInfo.fields.'System.WorkItemType'
    $changeType = $WorkItemInfo.fields.'Custom.ChangeType' 
    $releaseNotes = $WorkItemInfo.fields.'Custom.ReleaseNotes' 
    $releaseDate = $WorkItemInfo.fields.'Custom.ReleaseDate'
   
    $deployedVersion = $ReleaseVersion

   
    # If no release date is set, use the current date
    if ([string]::IsNullOrEmpty($releaseDate)) {
        $releaseDate = Get-Date -Format "yyyy-MM-dd"
    }
   
    # Format the information
    $formattedInfo = [PSCustomObject]@{
        WorkItemId = $WorkItemInfo.id
        WorkItemType = $workItemType
        Title = $WorkItemInfo.fields.'System.Title'
        CommitId = $null
        CommitMessage = $null
        CommitDate = $null
        PullRequestId = $PullRequestInfo.pullRequestId
        PullRequestTitle = $PullRequestInfo.title
        ChangeType = $changeType
        ReleaseNotes = $releaseNotes
        DeployedVersion = $deployedVersion
        ReleaseDate = $releaseDate
        Timestamp = $timestamp
    }
   
    return $formattedInfo
}

function Export-ReleaseNotesToMarkdown {
    param (
        [Array]$ReleaseNotes,
        [string]$OutputPath = "changelog.md",
        [string]$Organization,
        [string]$Project
    )
   
    # Group release notes by version
    $groupedNotes = $ReleaseNotes | Group-Object -Property DeployedVersion
   
    # Define emoji codes using Unicode escape sequences
    $breakingEmoji = [char]::ConvertFromUtf32(0x1F4A5)  # 💥
    $featureEmoji = [char]::ConvertFromUtf32(0x2728)    # ✨
    $fixEmoji = [char]::ConvertFromUtf32(0x1F527)       # 🔧
    $docsEmoji = [char]::ConvertFromUtf32(0x1F4DD)      # 📝
    $internalEmoji = [char]::ConvertFromUtf32(0x26A1)   # ⚡
    $bulletEmoji = [char]::ConvertFromUtf32(0x2022)     # •
    $unknownEmoji = [char]::ConvertFromUtf32(0x2753)    # ❓
   
    # Create changelog content with emoji icons
    $markdownContent = @"
# Changelog

The following contains all major, minor, and patch version release notes.

$bulletEmoji $breakingEmoji Breaking change!

$bulletEmoji $featureEmoji New Functionality

$bulletEmoji $fixEmoji Bug Fix

$bulletEmoji $docsEmoji Documentation Update

$bulletEmoji $internalEmoji Internal Optimization

$bulletEmoji $unknownEmoji Unspecified Change Type


"@
   
    # Sort versions (assuming semver format)
    $sortedGroups = $groupedNotes | Sort-Object -Property Name -Descending
   
    foreach ($versionGroup in $sortedGroups) {
        $version = $versionGroup.Name
        # Take the first release date from the group (should be the same for all items in a version)
        $releaseDate = ($versionGroup.Group | Select-Object -First 1).ReleaseDate
       
        # Add version header
        $markdownContent += @"

## Version $version

Release Date: $releaseDate


"@
       
        # Add each release note item
        foreach ($note in $versionGroup.Group) {
            # Map change type to emoji
            $icon = switch -Regex ($note.ChangeType) {
                "Breaking Change" { $breakingEmoji }
                "New Functionality" { $featureEmoji }
                "Bug Fix" { $fixEmoji }
                "Documentation Update" { $docsEmoji }
                "Internal Optimization" { $internalEmoji }
                default { $unknownEmoji }
            }
           
            # Clean up release notes text by removing HTML tags
            $cleanNotes = $note.ReleaseNotes
           
            # Replace common HTML patterns with Markdown equivalents
            if ($cleanNotes) {
                # Remove div tags
                $cleanNotes = $cleanNotes -replace '<div>', '' -replace '</div>', ''
               
                # Replace HTML lists with Markdown lists
                $cleanNotes = $cleanNotes -replace '<ul>', "`n"
                $cleanNotes = $cleanNotes -replace '</ul>', "`n"
                $cleanNotes = $cleanNotes -replace '<li>', '  * ' -replace '</li>', "`n"
               
                # Replace breaks with newlines + indent
                $cleanNotes = $cleanNotes -replace '<br>', "`n  "
               
                # Replace other common HTML tags
                $cleanNotes = $cleanNotes -replace '<[/]?(p|span|b|i|strong|em|h\d)[^>]*>', ''
               
                # Replace &quot; with actual quotes
                $cleanNotes = $cleanNotes -replace '&quot;', '"'
               
                # Remove any remaining HTML tags
                $cleanNotes = $cleanNotes -replace '<[^>]+>', ''
               
                # Trim whitespace
                $cleanNotes = $cleanNotes.Trim()
               
                # Normalize spacing (remove multiple consecutive blank lines)
                $cleanNotes = $cleanNotes -replace '(\r?\n){3,}', "`n`n"
               
                # Ensure proper indentation for multi-line notes
                if ($cleanNotes.Contains("`n")) {
                    $indentedLines = $cleanNotes -split "`n" | ForEach-Object {
                        if ($_ -match "^\s*\*$bulletEmoji") {
                            # Already a list item, leave as is
                            $_
                        } else {
                            # Add proper indentation to continuation lines
                            "  $_"
                        }
                    }
                    $cleanNotes = $indentedLines -join "`n"
                }
            } else {
                $cleanNotes = "No details provided"
            }

            function Format-GitUrl {
                param ([string]$value)
                return $value.Replace(' ', '%20')
            }
            
            $encodedProject = Format-GitUrl -value $Project
            # Add release note with work item link
            $markdownContent += "$bulletEmoji $icon $cleanNotes ([#$($note.WorkItemId)](https://dev.azure.com/$Organization/$encodedProject/_workitems/edit/$($note.WorkItemId)))`n"
            # add newline for readability
            $markdownContent += "`n"
        }
    }
   
    # Use .NET methods to write the file with proper UTF-8 encoding with BOM
    $utf8WithBom = New-Object System.Text.UTF8Encoding $true
    [System.IO.File]::WriteAllText($OutputPath, $markdownContent, $utf8WithBom)
   
    Write-Host "##[debug]Changelog exported to $OutputPath"

    return $markdownContent    
}

function Commit-ChangelogToReleaseBranch {
    param (
        [string]$ChangelogPath,
        [string]$BranchName,
        [string]$TempDir
    )
   
    Write-Host "##[section]Committing changelog to release branch"
   
    # Ensure we're in the right directory
    Set-Location $TempDir
   
    # Add the changelog file
    git add $ChangelogPath
    if ($LASTEXITCODE -ne 0) { throw "Failed to add changelog file to git" }
   
    # Commit
    git commit -m "docs: Add changelog for release $BranchName"
    if ($LASTEXITCODE -ne 0) { throw "Failed to commit changelog" }
   
    # Push
    git push origin $BranchName
    if ($LASTEXITCODE -ne 0) { throw "Failed to push changelog to remote" }
   
    Write-Host "##[debug]Successfully committed and pushed changelog to $BranchName"
}

# Enhanced helper function to extract channel ID from Teams URL
function Get-ChannelIdFromUrl {
    param ([string]$TeamsUrl)
   
    try {
        Write-Host "##[debug]Extracting channel ID from URL: $TeamsUrl"
       
        # Pattern 1: teams.microsoft.com/l/channel/CHANNEL_ID/
        if ($TeamsUrl -match "teams\.microsoft\.com/l/channel/([^/\?&%]+)") {
            $channelId = [System.Web.HttpUtility]::UrlDecode($matches[1])
            Write-Host "##[debug]Found channel ID (pattern 1): $channelId"
            return $channelId
        }
       
        # Pattern 2: Look for channelId in query parameters
        $uri = [Uri]::new($TeamsUrl)
        $queryParams = [System.Web.HttpUtility]::ParseQueryString($uri.Query)
        $channelId = $queryParams["channelId"]
       
        if ($channelId) {
            Write-Host "##[debug]Found channel ID (query param): $channelId"
            return $channelId
        }
       
        # Pattern 3: Encoded channel ID in URL path
        if ($TeamsUrl -match "/([0-9a-fA-F-]{36})/" -or $TeamsUrl -match "/19%3A[^/]+/") {
            $channelId = [System.Web.HttpUtility]::UrlDecode($matches[1])
            Write-Host "##[debug]Found channel ID (pattern 3): $channelId"
            return $channelId
        }
       
        Write-Warning "Could not extract channel ID from URL: $TeamsUrl"
        return $null
    }
    catch {
        Write-Warning "Could not extract channel ID from URL: $TeamsUrl - Error: $($_.Exception.Message)"
        return $null
    }
}

function Get-TeamsToken {
    try {
        $tenantId = "$env:ARM_TENANT_ID"
        $clientId = "$env:TEAMS_CLIENT_ID"        # Your custom app ID
        $username = "$env:TEAMS_NOTIFICATION_USERNAME"
        $password = "$env:TEAMS_NOTIFICATION_PASSWORD"
        $clientSecret = "$env:TEAMS_CLIENT_SECRET" # Your custom app secret

        $body = @{
            grant_type = "password"
            client_id = $clientId
            client_secret = $clientSecret
            resource = "https://graph.microsoft.com/"
            username = $username
            password = $password
            scope = "ChannelMessage.Send User.Read"
        }
       
        $uri = "https://login.microsoftonline.com/$tenantId/oauth2/token"

        Write-Host "##[debug]Requesting Teams token with ChannelMessage.Send scope"
        Write-Host "##[debug]Request URI: $uri"
        Write-Host "##[debug]Request Body: $($body | ConvertTo-Json -Depth 10)"

        $response = Invoke-RestMethod -Uri $uri -Method POST -Body $body -ContentType "application/x-www-form-urlencoded"
       
        return $response.access_token
    }
    catch {
        Write-Error "Failed to get Teams token with ChannelMessage.Send: $_"
        throw
    }
}

function Get-TeamsChannelInfo {
    param ([string]$TeamsUrl)
   
    Write-Host "##[debug]Parsing Teams URL: $TeamsUrl"
   
    try {
        $channelInfo = @{
            TeamId = $null
            ChannelId = $null
            IsValid = $false
            ErrorMessage = ""
            RawChannelId = $null
            DecodedChannelId = $null
        }
       
        # Parse the URL
        $uri = [Uri]::new($TeamsUrl)
        Write-Host "##[debug]Parsed URI Path: $($uri.AbsolutePath)"
        Write-Host "##[debug]Parsed URI Query: $($uri.Query)"
       
        # Extract query parameters first (most reliable for Team ID)
        $queryParams = [System.Web.HttpUtility]::ParseQueryString($uri.Query)
       
        # Get Team ID from query parameters
        $teamIdFromQuery = $queryParams["groupId"]
        Write-Host "##[debug]Team ID from query: $teamIdFromQuery"
       
        if ($teamIdFromQuery) {
            $channelInfo.TeamId = $teamIdFromQuery
        }
       
        # Extract Channel ID from URL path
        # Pattern: /l/channel/ENCODED_CHANNEL_ID/CHANNEL_NAME
        if ($uri.AbsolutePath -match "/l/channel/([^/]+)/") {
            $rawChannelId = $matches[1]
            $channelInfo.RawChannelId = $rawChannelId
           
            Write-Host "##[debug]Raw Channel ID (URL encoded): $rawChannelId"
           
            # URL decode the channel ID
            $decodedChannelId = [System.Web.HttpUtility]::UrlDecode($rawChannelId)
            $channelInfo.DecodedChannelId = $decodedChannelId
            $channelInfo.ChannelId = $decodedChannelId
           
            Write-Host "##[debug]Decoded Channel ID: $decodedChannelId"
        }
       
        # Validate the extracted IDs
        $channelInfo.IsValid = $true
        $validationErrors = @()
       
        # Validate Team ID (should be a GUID)
        if (-not $channelInfo.TeamId) {
            $validationErrors += "Team ID not found"
            $channelInfo.IsValid = $false
        } elseif ($channelInfo.TeamId -notmatch "^[0-9a-fA-F-]{36}$") {
            $validationErrors += "Team ID format invalid: $($channelInfo.TeamId)"
            $channelInfo.IsValid = $false
        }
       
        # Validate Channel ID (should start with 19: and end with @thread)
        if (-not $channelInfo.ChannelId) {
            $validationErrors += "Channel ID not found"
            $channelInfo.IsValid = $false
        } elseif ($channelInfo.ChannelId -notmatch "^19:.*@thread") {
            $validationErrors += "Channel ID format invalid: $($channelInfo.ChannelId)"
            $channelInfo.IsValid = $false
        }
       
        $channelInfo.ErrorMessage = $validationErrors -join "; "
       
        Write-Host "##[debug]Final Team ID: $($channelInfo.TeamId)"
        Write-Host "##[debug]Final Channel ID: $($channelInfo.ChannelId)"
        Write-Host "##[debug]Validation Result: $($channelInfo.IsValid)"
       
        if (-not $channelInfo.IsValid) {
            Write-Host "##[debug]Validation Errors: $($channelInfo.ErrorMessage)"
        }
       
        return $channelInfo
    }
    catch {
        Write-Warning "Error parsing Teams URL: $($_.Exception.Message)"
        return @{
            TeamId = $null
            ChannelId = $null
            IsValid = $false
            ErrorMessage = "Exception: $($_.Exception.Message)"
            RawChannelId = $null
            DecodedChannelId = $null
        }
    }
}

function Convert-MarkdownLinksToHtml {
    param([string]$Text)
   
    try {
       
        # Use a more specific regex that handles multiple links in one line
        $linkPattern = '\[([^\[\]]+)\]\(([^()]+)\)'
       
        # Process each match individually to avoid issues
        $result = $Text
        while ($result -match $linkPattern) {
            $fullMatch = $matches[0]
            $linkText = $matches[1]
            $linkUrl = $matches[2]
           
            # Replace only this specific match
            $htmlLink = "<a href=`"$linkUrl`">$linkText</a>"
            $result = $result -replace [regex]::Escape($fullMatch), $htmlLink
           
            Write-Host "##[debug]Converted link: [$linkText]($linkUrl) -> $htmlLink"
        }
       
        return $result
    }
    catch {
        Write-Warning "Error converting markdown links: $_"
        return $Text
    }
}


function Format-MarkdownForTeams {
    param(
        [string]$MarkdownContent,
        [string]$Repository,
        [string]$ReleaseVersion,
        [hashtable]$Emojis
    )
   
    try {
        Write-Host "##[debug]Formatting markdown content for Teams"
       
        # Define all emojis using HTML entities for better Teams compatibility
        $breakingEmoji = "&#x1F4A5;"    # 💥 Breaking change
        $featureEmoji = "&#x2728;"      # ✨ New Functionality  
        $fixEmoji = "&#x1F527;"         # 🔧 Bug Fix
        $docsEmoji = "&#x1F4DD;"        # 📝 Documentation Update
        $internalEmoji = "&#x26A1;"     # ⚡ Internal Optimization
        $bulletEmoji = "&#x2022;"       # • Bullet point
        $unknownEmoji = "&#x2753;"      # ❓ Unspecified Change Type
        $rocketEmoji = "&#x1F680;"      # 🚀 Rocket
        $robotEmoji = "&#x1F916;"       # 🤖 Robot
        $ccEmoji = "&#x1F4E7;"          # 📧 Email/CC symbol

        # Create release header for Teams with HTML formatting for proper line breaks
        $teamsHeader = @"
<p><strong>$rocketEmoji Release Notification</strong></p>
<p><strong>Repository:</strong> $Repository<br/>
<strong>Version:</strong> $ReleaseVersion<br/>
<strong>Release Date:</strong> $(Get-Date -Format "yyyy-MM-dd")</p>
<p><strong>Changelog</strong></p>
<p>The following contains all major, minor, and patch version release notes.</p>
<ul>
<li> $breakingEmoji Breaking change!</li>
<li> $featureEmoji New Functionality</li>
<li> $fixEmoji Bug Fix</li>
<li> $docsEmoji Documentation Update</li>
<li> $internalEmoji Internal Optimization</li>
<li> $unknownEmoji Unspecified Change Type</li>
</ul>
"@
       
        # Extract version sections from markdown content
        $lines = $MarkdownContent -split '\r?\n'
        $versionContent = ""
        $inVersionSection = $false
       
        foreach ($line in $lines) {
            # Look for version headers
            if ($line -match '^## Version (.+)$') {
                $versionNumber = $matches[1]
                $versionContent += "<h3>Version $versionNumber</h3>`n"
                $inVersionSection = $true
                continue
            }
           
            # Look for release date
            if ($line -match '^Release Date: (.+)$' -and $inVersionSection) {
                $releaseDate = $matches[1]
                $versionContent += "<p>Release Date: $releaseDate</p>`n"
                continue
            }
           
            # Process release note items with emojis - using ConvertFromUtf32 values for comparison
            if ($line -match '^. (.+)$' -and $inVersionSection) {
                $noteContent = $matches[1]
               
                # Replace emoji characters with HTML entities using ConvertFromUtf32 for matching
                $noteContent = $noteContent -replace [char]::ConvertFromUtf32(0x1F4A5), $breakingEmoji    # 💥
                $noteContent = $noteContent -replace [char]::ConvertFromUtf32(0x2728), $featureEmoji     # ✨
                $noteContent = $noteContent -replace [char]::ConvertFromUtf32(0x1F527), $fixEmoji        # 🔧
                $noteContent = $noteContent -replace [char]::ConvertFromUtf32(0x1F4DD), $docsEmoji       # 📝
                $noteContent = $noteContent -replace [char]::ConvertFromUtf32(0x26A1), $internalEmoji   # ⚡
                $noteContent = $noteContent -replace [char]::ConvertFromUtf32(0x2753), $unknownEmoji    # ❓
               
                Write-Host "##[debug]After emoji replacement: $noteContent"
               
                # Convert markdown links to HTML links using the new function
                $noteContent = Convert-MarkdownLinksToHtml -Text $noteContent
               
                Write-Host "##[debug]After link conversion: $noteContent"
               
                $noteContent = $noteContent -replace '&(?!#|[a-zA-Z]+;)', '&amp;'  
               
                $versionContent += "<p>$noteContent</p>`n"
                continue
            }
           
            # Handle non-bullet point lines that might contain content
            if ($inVersionSection -and $line.Trim() -ne '' -and -not ($line -match '^#')) {
                # This might be a continuation line or other content
                $cleanLine = $line.Trim()
                if ($cleanLine -ne '') {
                    # Convert markdown links to HTML links in continuation lines too
                    $cleanLine = Convert-MarkdownLinksToHtml -Text $cleanLine
                   
                    # Selective HTML encoding
                    $cleanLine = $cleanLine -replace '&(?!#|[a-zA-Z]+;)', '&amp;'
                   
                    $versionContent += "<p>$cleanLine</p>`n"
                }
                continue
            }
           
            # Skip empty lines and headers we don't want
            if ($line -match '^#' -or $line.Trim() -eq '') {
                continue
            }
        }
       
        # If no version content was found, create a fallback
        if ([string]::IsNullOrWhiteSpace($versionContent)) {
            $versionContent = "<h3>Version $ReleaseVersion</h3>`n<p>Release Date: $(Get-Date -Format "yyyy-MM-dd")</p>`n<p>No release notes available for this version.</p>`n"
        }
       
        # Combine all content
        $finalContent = $teamsHeader + $versionContent
       
        Write-Host "##[debug]Final Teams content length: $($finalContent.Length) characters"
       
        return $finalContent
    }
    catch {
        Write-Warning "Error formatting markdown for Teams: $_"
        # Fallback to simple format if processing fails - using HTML entities for safety
        $fallbackRocket = "&#x1F680;"  # 🚀
        $fallbackRobot = "&#x1F916;"   # 🤖
        return "<p><strong>$fallbackRocket Release Notification</strong></p><p>Repository: $Repository<br/>Version: $ReleaseVersion</p><p>$MarkdownContent</p><hr/><p><em>$fallbackRobot Automated release notification from Azure DevOps</em></p>"
    }
}

function Get-TeamsChannelTags {
    param(
        [Parameter(Mandatory=$true)]
        [string]$TeamId,
       
        [Parameter(Mandatory=$true)]
        [string]$Token
    )
   
    try {
        $headers = @{
            "Authorization" = "Bearer $Token"
            "Content-Type" = "application/json"
        }
       
        Write-Host "##[debug]Getting tags for Team: $TeamId"
       
        # Get all tags for the team
        $tagsUrl = "https://graph.microsoft.com/v1.0/teams/$TeamId/tags"
       
        $response = Invoke-RestMethod -Uri $tagsUrl -Headers $headers -Method GET
       
        $tags = @()
       
        if ($response.value) {
            foreach ($tag in $response.value) {
                $tagInfo = @{
                    id = $tag.id
                    displayName = $tag.displayName
                    description = $tag.description
                    memberCount = $tag.memberCount
                    tagType = $tag.tagType
                    teamId = $TeamId
                }
               
                $tags += $tagInfo
            }
        }
       
        Write-Host "##[info]Found $($tags.Count) tags in the team"
        return $tags
       
    } catch {
        Write-Error "Failed to get team tags: $($_.Exception.Message)"
       
        if ($_.Exception.Response) {
            $statusCode = $_.Exception.Response.StatusCode
            Write-Error "HTTP Status Code: $statusCode"
           
            if ($statusCode -eq 404) {
                Write-Warning "Tags may not be available for this team or you may lack permissions"
            }
           
            try {
                $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $responseBody = $reader.ReadToEnd()
                Write-Error "Response body: $responseBody"
            }
            catch {
                Write-Warning "Could not read error response body"
            }
        }
       
        return @()
    }
}


function Send-TeamsChannelMessage {
    param(
        [string]$TeamId,
        [string]$ChannelId,
        [Parameter(Mandatory=$true)]
        [string]$MarkdownContent,
        [string]$Repository = "Unknown Repository",
        [string]$ReleaseVersion = "Unknown Version",
        [string]$Token   
    )
   
    try {
        # Define emoji variables using HTML entities for Azure DevOps compatibility
        $bulletEmoji = "&#x2022;"       # •
        $featureEmoji = "&#x2728;"      # ✨
        $fixEmoji = "&#x1F527;"         # 🔧
       
        $headers = @{
            "Authorization" = "Bearer $Token"
            "Content-Type" = "application/json; charset=utf-8"
        }
       
        # Get all available tags
        Write-Host "##[section]Getting Teams tags"
        $availableTags = Get-TeamsChannelTags -TeamId $TeamId -Token $Token

        if ($availableTags.Count -eq 0) {
            Write-Warning "No tags found in this team"
        }

        # display available tags
        Write-Host "##[debug]Available tags: $($availableTags | ConvertTo-Json -Depth 10)"

        $tagsToMention = $availableTags

        # Format the Teams message content using the provided markdown content
        $formattedContent = Format-MarkdownForTeams -MarkdownContent $MarkdownContent -Repository $Repository -ReleaseVersion $ReleaseVersion -Emojis @{
            Rocket = "&#x1F680;"  # 🚀
            Robot = "&#x1F916;"   # 🤖
            Bullet = $bulletEmoji
        }

        # Create mentions and CC footer
        $mentions = @()
        $mentionId = 0
        $tagMentionsVisual = @()
        $tagMentionsNotification = @()

        foreach ($tag in $tagsToMention) {
            # $TEAMS_TAGS are comma delimiter, we need to test if either matches $tag.displayName
            if ($env:TEAMS_TAGS -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ -eq $tag.displayName }) {

                # Create actual mention for notifications
                $mention = @{
                    "id" = $mentionId
                    "mentionText" = $tag.displayName
                    "mentioned" = @{
                        "tag" = @{
                            "id" = $tag.id
                            "displayName" = $($tag.displayName)
                        }
                    }
                }
            
                $mentions += $mention
                $tagMentionsNotification += "<at id=`"$mentionId`">$($tag.displayName)</at>"                
                $mentionId++
            }

        }
        if ($($mentions.Count) -eq 1) {
            $mentionsJson = "[$($mentions | ConvertTo-Json -Depth 10)]"
        } else {
        
            # convert mentions to json
            if ($($mentions.Count) -eq 0) {
                $mentionsJson = $null
            } else {
                $mentionsJson = $mentions | ConvertTo-Json -Depth 10
            }
        }

        # Create footer
        $ccEmoji = "&#x1F4E2;" # 📢 
        $robotEmoji = "&#x1F916;"
    
        $ccText = if ($tagMentionsNotification.Count -gt 0) {
            $tagMentionsNotification -join ", "
        } else {
            "No tags available"
        }
    
        $ccFooter = @"
        <hr/>
        <p><strong>$ccEmoji CC (Notifications):</strong> $ccText</p>
        <hr/>        
        <p><em>$robotEmoji Automated release notification from Azure DevOps</em></p>
"@        
        $formattedContent += $ccFooter

        # Escape any remaining quotes for JSON but preserve HTML
        $escapedContent = $formattedContent -replace '"', '\"'
       
        # Create the Teams message payload using HTML content type
        $BodyJsonTeam = ""
       if ($availableTags.Count -ne 0) {
            $BodyJsonTeam = @"
{
    "subject": "Release Notification - $ReleaseVersion",
    "importance": "high",
    "body": {
        "contentType": "html",
        "content": "$escapedContent"        
    },
    "mentions": $mentionsJson
}
"@
         } else {
            $BodyJsonTeam = @"
{
    "subject": "Release Notification - $ReleaseVersion",
    "importance": "high",
    "body": {
        "contentType": "html",
        "content": "$escapedContent"        
    }
}
"@
         }
         # Construct the API URL
        $messageUrl = "https://graph.microsoft.com/beta/teams/$TeamId/channels/$ChannelId/messages"
       
        Write-Host "##[debug]Sending message to Teams channel: $messageUrl"
        Write-Host "##[debug]Message payload: $BodyJsonTeam"
        Write-Host "##[debug]Message length: $($formattedContent.Length) characters"
        Write-Host "##[debug]Repository: $Repository"
        Write-Host "##[debug]Version: $ReleaseVersion"
       
       
        # Debug: Show first 300 characters of the formatted content
        Write-Host "##[debug]Formatted content preview: $($formattedContent.Substring(0, [Math]::Min(300, $formattedContent.Length)))..."
       
        $response = Invoke-RestMethod -Uri $messageUrl -Method POST -Headers $headers -Body $BodyJsonTeam -ErrorAction Stop
       
        Write-Host "Message sent successfully to Teams channel with HTML entities"
        Write-Host "##[debug]Message ID: $($response.id)"
        return $true
    }
    catch {
        Write-Error "Failed to send Teams message: $($_.Exception.Message)"
       
        # Enhanced error handling
        if ($_.Exception.Response) {
            $statusCode = $_.Exception.Response.StatusCode
            Write-Error "HTTP Status Code: $statusCode"
           
            try {
                $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $responseBody = $reader.ReadToEnd()
                Write-Error "Response body: $responseBody"
            }
            catch {
                Write-Warning "Could not read error response body"
            }
        }
       
        return $false
    }
}

# Main execution flow
try {

    # Set full version string
    $version = "$ReleaseVersion"
   
    # Define emoji codes using Unicode escape sequences for console output
    $rocketEmoji = [char]::ConvertFromUtf32(0x1F680)      # 🚀
    $checkEmoji = [char]::ConvertFromUtf32(0x2713)        # ✓
    $crossEmoji = [char]::ConvertFromUtf32(0x274C)        # ❌
    $infoEmoji = [char]::ConvertFromUtf32(0x2139)         # ℹ️
    $warningEmoji = [char]::ConvertFromUtf32(0x26A0)      # ⚠️
    $gearEmoji = [char]::ConvertFromUtf32(0x2699)         # ⚙️
    $folderEmoji = [char]::ConvertFromUtf32(0x1F4C1)      # 📁
    $documentEmoji = [char]::ConvertFromUtf32(0x1F4C4)    # 📄
    $bellEmoji = [char]::ConvertFromUtf32(0x1F514)        # 🔔

    # trim leading and trailing spaces for Project and Repository
    $Project = $Project.Trim()
    $Repository = $Repository.Trim()
    
    # Validate required environment variables
    Write-Host "##[section]$gearEmoji Validating environment variables"
    $requiredEnvVars = @("ARM_CLIENT_ID", "ARM_CLIENT_SECRET", "ARM_CLIENT_OBJECT_ID", "ARM_TENANT_ID")
    foreach ($envVar in $requiredEnvVars) {
        if ([string]::IsNullOrEmpty((Get-Item "env:$envVar" -ErrorAction SilentlyContinue).Value)) {
            Write-Host "##[error]$crossEmoji Required environment variable $envVar is not set"
            throw "Required environment variable $envVar is not set"
        } else {
            Write-Host "##[debug]$checkEmoji $envVar is set"
        }
    }
    # Show Service Principal Object ID that will be used
    Write-Host "##[debug]$infoEmoji Service Principal Object ID: $env:ARM_CLIENT_OBJECT_ID"
    Write-Host "##[debug]$bellEmoji Teams Channel Web URL: $TeamsChannelWebUrl"

    # Get token and validate repository
    $token = Get-DevOpsAuthToken
    Test-GitRepository
   
    # Using splatting for Initialize-GitConfiguration
    $gitConfig = @{
        Token = $Token
        Organization = $Organization
        Project = $Project
        Repository = $Repository
    }
    Initialize-GitConfiguration @gitConfig
   
    # Look up Project and Repository IDs
    Write-Host "##[section]Looking up Project and Repository IDs"
    $projectId = Get-ProjectId -Organization $Organization -ProjectName $Project -Token $token
    $repositoryId = Get-RepositoryId -Organization $Organization -ProjectId $projectId -RepositoryName $Repository -Token $token  
   
    # Create release branch using IDs
    $branchInfo = New-ReleaseBranch -Version $version -SourceBranch $SourceBranch -Organization $Organization -ProjectId $projectId -RepositoryId $repositoryId
    $branchName = $branchInfo.BranchName
    $tempDir = $branchInfo.TempDir

    Write-Host "##[section]Generating changelog for release $version"
   
    # Get completed pull requests for the specified period
    $pullRequests = Get-CompletedPullRequests -Organization $Organization -ProjectId $projectId -RepositoryId $repositoryId -DaysToLookBack $DaysToLookBack -Token $token

    $releaseNotes = @()
   
    # Process each pull request to get work items
    foreach ($pr in $pullRequests) {
        Write-Host "##[debug]Processing pull request #$($pr.pullRequestId): $($pr.title)"
       
        # Get work items associated with this pull request
        $workItems = Get-WorkItemsForPullRequest -Organization $Organization -ProjectId $projectId -RepositoryId $repositoryId -PullRequestId $pr.pullRequestId -Token $token
       
        foreach ($workItem in $workItems) {
            # Get full work item details
            $workItemDetails = Get-WorkItemDetails -Organization $Organization -ProjectId $projectId -WorkItemId $workItem.id -Token $token
           
            # Check if work item is a Product Backlog Item
            if ($workItemDetails -and $workItemDetails.fields -and $workItemDetails.fields.'System.WorkItemType' -eq 'Product Backlog Item') {
                Write-Host "##[debug]    Found Product Backlog Item #$($workItemDetails.id): $($workItemDetails.fields.'System.Title')"
               
                # Format and collect release notes
                $formattedInfo = Format-ReleaseNotes -PullRequestInfo $pr -WorkItemInfo $workItemDetails
                $releaseNotes += $formattedInfo
            }
            elseif ($workItemDetails -and ($workItemDetails.fields -and $workItemDetails.fields.'System.WorkItemType' -eq 'Bug') -and ($workItemDetails.fields.'System.State' -eq 'Resolved' -or $workItemDetails.fields.'System.State' -eq 'Done')) {
                # Format and collect release notes
                $formattedInfo = Format-ReleaseNotes -PullRequestInfo $pr -WorkItemInfo $workItemDetails
                $releaseNotes += $formattedInfo                
            }
            elseif ($workItemDetails) {
                $workItemType = $workItemDetails.fields.'System.WorkItemType'
                Write-Host "##[debug]    Skipping work item #$($workItemDetails.id): not a Product Backlog Item (type: $workItemType)"
            }
        }
    }
   
    # Export results to changelog.md
    $changelogPath = Join-Path $tempDir "changelog.md"
    if ($releaseNotes.Count -gt 0) {
        Write-Host "##[section]Found $($releaseNotes.Count) release notes entries"
        $markdownContent = Export-ReleaseNotesToMarkdown -ReleaseNotes $releaseNotes -OutputPath $changelogPath -Organization $Organization -Project $Project
       
        # Commit and push changelog to release branch
        Commit-ChangelogToReleaseBranch -ChangelogPath $changelogPath -BranchName $branchName -TempDir $tempDir

        # Send Teams notification via Graph API if enabled
        if ($SendTeamsNotification) {
            Write-Host "##[section]$bellEmoji Sending Teams notification"
            $token = Get-TeamsToken
            $channelInfo = Get-TeamsChannelInfo -TeamsUrl $TeamsChannelWebUrl
            $teamId = $channelInfo.TeamId
            $channelId = $channelInfo.ChannelId
            Send-TeamsChannelMessage -TeamId $teamId -ChannelId $channelId -MarkdownContent $markdownContent -Repository $Repository -ReleaseVersion $version -Token $token
            
        } else {
            Write-Host "##[debug]$infoEmoji Teams notification is disabled"
        }

    }
    else {
       Write-Host "##[section]$warningEmoji No release notes found, creating empty changelog"
        # Define emojis for empty changelog using Unicode escape sequences
        $boomEmoji = [char]::ConvertFromUtf32(0x1F4A5)        # 💥
        $sparklesEmoji = [char]::ConvertFromUtf32(0x2728)     # ✨
        $wrenchEmoji = [char]::ConvertFromUtf32(0x1F527)      # 🔧
        $docsEmoji = [char]::ConvertFromUtf32(0x1F4DD)        # 📝
        $zaptEmoji = [char]::ConvertFromUtf32(0x26A1)         # ⚡
       
        $emptyChangelogContent = @"
# Changelog

The following contains all major, minor, and patch version release notes.

- $boomEmoji Breaking change!
- $sparklesEmoji New Functionality
- $wrenchEmoji Bug Fix
- $docsEmoji Documentation Update
- $zaptEmoji Internal Optimization

## Version $version
Release Date: $(Get-Date -Format "yyyy-MM-dd")

No release notes available for this version.
"@
        Set-Content -Path $changelogPath -Value $emptyChangelogContent -Encoding UTF8
       
        # Commit and push changelog to release branch
        Commit-ChangelogToReleaseBranch -ChangelogPath $changelogPath -BranchName $branchName -TempDir $tempDir
    }
   
    # Output variables for Azure DevOps pipeline
    Write-Host "##vso[task.setvariable variable=ReleaseBranchName;isoutput=true]$branchName"
    Write-Host "##vso[task.setvariable variable=ReleaseVersion;isoutput=true]$version"
    Write-Host "##vso[task.setvariable variable=ReleaseNotesCount;isoutput=true]$($releaseNotes.Count)"
   
    Write-Host "##[section]$rocketEmoji Release branch creation completed successfully"
    Write-Host "$checkEmoji Release Branch: $branchName"
    Write-Host "$checkEmoji Version: $version"
    Write-Host "$checkEmoji Release Notes Count: $($releaseNotes.Count)"
    Write-Host "$checkEmoji Teams Notification: $(if ($SendTeamsNotification) { 'Enabled' } else { 'Disabled' })"
}
catch {
    Write-Error $_
    exit 1
}
finally {
    # Clean up
    if (Test-Path "$HOME/.git-credentials") {
        Remove-Item "$HOME/.git-credentials" -Force
    }
   
    # Return to original location if needed
    if (Get-Location -Stack -ErrorAction SilentlyContinue) {
        Pop-Location
    }
}
