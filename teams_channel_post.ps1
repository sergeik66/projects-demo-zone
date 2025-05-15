# Configuration variables
$tenantId = "your-tenant-id"  # Your Azure AD Tenant ID
$clientId = "your-client-id"  # Your Azure AD App Client ID
$clientSecret = "your-client-secret"  # Your Azure AD App Client Secret
$teamId = "your-team-id"  # The ID of the Team
$channelId = "your-channel-id"  # The ID of the Channel

# Message parameters
$subject = "Weekly Team Update"
$postType = "Announcement"  # Options: Announcement, Update, Question
$messageContent = "Here's the latest update for our project. Please review and share feedback!"
$filePath = "C:\Path\To\Your\File.pdf"  # Optional: Path to the file to attach (set to $null if no attachment)

# Validate post type
$validPostTypes = "Announcement", "Update", "Question"
if ($postType -notin $validPostTypes) {
    Write-Error "Invalid post type. Choose from: $validPostTypes"
    exit
}

# Get an access token
$tokenUrl = "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
$tokenBody = @{
    grant_type    = "client_credentials"
    scope         = "https://graph.microsoft.com/.default"
    client_id     = $clientId
    client_secret = $clientSecret
}
try {
    $tokenResponse = Invoke-RestMethod -Uri $tokenUrl -Method POST -Body $tokenBody
    $accessToken = $tokenResponse.access_token
}
catch {
    Write-Error "Failed to obtain access token: $_"
    exit
}

# Build the message template
$messageTemplate = "<h3>[$postType] $subject</h3><p>$messageContent</p>"
$bodyContent = @{
    contentType = "html"
    content     = $messageTemplate
}

# Handle file attachment (if provided)
$attachments = @()
if ($filePath -and (Test-Path $filePath)) {
    try {
        # Upload file to the Teams channel's Files tab (SharePoint document library)
        $fileName = Split-Path $filePath -Leaf
        $fileBytes = [System.IO.File]::ReadAllBytes($filePath)
        $uploadUrl = "https://graph.microsoft.com/v1.0/teams/$teamId/channels/$channelId/filesFolder/driveItem/children/$fileName/content"
        $headers = @{
            "Authorization" = "Bearer $accessToken"
            "Content-Type"  = "application/octet-stream"
        }
        $uploadResponse = Invoke-RestMethod -Uri $uploadUrl -Method PUT -Headers $headers -Body $fileBytes
        $fileId = $uploadResponse.id

        # Add attachment reference to the message
        $attachments += @{
            id              = $fileId
            contentType     = "reference"
            contentUrl      = $uploadResponse.webUrl
            name            = $fileName
            thumbnailUrl    = $null
        }
    }
    catch {
        Write-Warning "Failed to upload file: $_"
    }
}

# Prepare the Graph API request to post the message
$graphUrl = "https://graph.microsoft.com/v1.0/teams/$teamId/channels/$channelId/messages"
$headers = @{
    "Authorization" = "Bearer $accessToken"
    "Content-Type"  = "application/json"
}
$body = @{
    body        = $bodyContent
    attachments = $attachments
} | ConvertTo-Json -Depth 4

# Send the message
try {
    $response = Invoke-RestMethod -Uri $graphUrl -Method POST -Headers $headers -Body $body
    Write-Output "Message posted successfully! Message ID: $($response.id)"
}
catch {
    Write-Error "Failed to post message: $_"
}

# Alternative: Webhook with Template

$webhookUrl = "your-webhook-url"
$subject = "Weekly Team Update"
$postType = "Announcement"
$messageContent = "Here's the latest update for our project."

# Build the message template
$messageTemplate = "<h3>[$postType] $subject</h3><p>$messageContent</p>"
$message = @{
    textFormat = "html"
    text       = $messageTemplate
} | ConvertTo-Json

# Send the message
Invoke-RestMethod -Method POST -ContentType 'application/json' -Body $message -Uri $webhookUrl
