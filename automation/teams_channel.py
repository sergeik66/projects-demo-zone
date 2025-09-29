import argparse
import os
import subprocess
import json
import requests
import msal
import re
import tempfile
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path
import html
from typing import Dict, List, Any, Optional

# Argument parsing (equivalent to PowerShell parameters)
parser = argparse.ArgumentParser(description="Generate release notes and post to Teams")
parser.add_argument("--ReleaseVersion", required=True, help="Release version (e.g., 1.2.3)")
parser.add_argument("--SourceBranch", default="main", help="Source branch for release")
parser.add_argument("--Organization", required=True, help="Azure DevOps organization")
parser.add_argument("--Project", required=True, help="Azure DevOps project")
parser.add_argument("--Repository", required=True, help="Azure DevOps repository")
parser.add_argument("--ProjectPrefix", default="data-product", help="Project prefix")
parser.add_argument("--DaysToLookBack", type=int, default=8, help="Days to look back for PRs")
parser.add_argument("--TeamsChannelWebUrl", help="Teams channel URL for notifications")
parser.add_argument("--SendTeamsNotification", action="store_true", help="Enable Teams notification")
args = parser.parse_args()

# Validate environment variables
required_env_vars = ["ARM_CLIENT_ID", "ARM_CLIENT_SECRET", "ARM_TENANT_ID"]
for var in required_env_vars:
    if not os.getenv(var):
        print(f"##[error] Required environment variable {var} is not set")
        exit(1)
    print(f"##[debug] {var} is set")

if args.SendTeamsNotification:
    required_teams_vars = ["TEAMS_CLIENT_ID", "TEAMS_CLIENT_SECRET"]
    for var in required_teams_vars:
        if not os.getenv(var):
            print(f"##[error] Required Teams environment variable {var} is not set")
            exit(1)
        print(f"##[debug] {var} is set")

# Emoji definitions (Unicode for console, HTML entities for Teams)
EMOJIS = {
    "rocket": "\U0001F680",  # 🚀
    "check": "\U00002713",   # ✓
    "cross": "\U0000274C",   # ❌
    "info": "\U00002139",    # ℹ️
    "warning": "\U000026A0", # ⚠️
    "gear": "\U00002699",    # ⚙️
    "folder": "\U0001F4C1",  # 📁
    "document": "\U0001F4C4",# 📄
    "bell": "\U0001F514",    # 🔔
    "breaking": "\U0001F4A5",# 💥
    "feature": "\U00002728", # ✨
    "fix": "\U0001F527",     # 🔧
    "docs": "\U0001F4DD",    # 📝
    "internal": "\U000026A1",# ⚡
    "unknown": "\U00002753", # ❓
    "bullet": "\U00002022",  # •
    "cc": "\U0001F4E7",      # 📧
    "robot": "\U0001F916"    # 🤖
}

# HTML entities for Teams compatibility
TEAMS_EMOJIS = {
    "breaking": "&#x1F4A5;",  # 💥
    "feature": "&#x2728;",    # ✨
    "fix": "&#x1F527;",       # 🔧
    "docs": "&#x1F4DD;",      # 📝
    "internal": "&#x26A1;",   # ⚡
    "unknown": "&#x2753;",    # ❓
    "bullet": "&#x2022;",     # •
    "rocket": "&#x1F680;",    # 🚀
    "robot": "&#x1F916;",     # 🤖
    "cc": "&#x1F4E2;"         # 📢
}

def get_devops_auth_token() -> str:
    """Authenticate to Azure DevOps using Client Credentials flow."""
    print("##[section] Authenticating to Azure DevOps")
    auth_url = f"https://login.microsoftonline.com/{os.getenv('ARM_TENANT_ID')}/oauth2/token"
    body = {
        "grant_type": "client_credentials",
        "client_id": os.getenv("ARM_CLIENT_ID"),
        "client_secret": os.getenv("ARM_CLIENT_SECRET"),
        "resource": "499b84ac-1321-427f-aa17-267ca6975798"
    }
    try:
        response = requests.post(auth_url, data=body, timeout=30)
        response.raise_for_status()
        return response.json()["access_token"]
    except Exception as e:
        print(f"##[error] Failed to get Azure DevOps token: {e}")
        raise

def get_teams_token() -> str:
    """Authenticate to Microsoft Graph using Client Credentials flow."""
    print("##[section] Authenticating to Microsoft Graph")
    app = msal.ConfidentialClientApplication(
        client_id=os.getenv("TEAMS_CLIENT_ID"),
        client_credential=os.getenv("TEAMS_CLIENT_SECRET"),
        authority=f"https://login.microsoftonline.com/{os.getenv('ARM_TENANT_ID')}"
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" not in result:
        print(f"##[error] Failed to get Teams token: {result.get('error_description')}")
        raise Exception(f"Authentication failed: {result.get('error_description')}")
    return result["access_token"]

def test_git_repository():
    """Verify git is installed."""
    try:
        subprocess.run(["git", "--version"], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("##[error] Git is not installed")
        raise Exception("Git is not installed")

def get_project_id(organization: str, project_name: str, token: str) -> str:
    """Get Azure DevOps project ID."""
    print(f"##[debug] Looking up Project ID for: {project_name}")
    uri = f"https://dev.azure.com/{organization}/_apis/projects?api-version=7.0"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(uri, headers=headers, timeout=30)
    response.raise_for_status()
    projects = response.json()["value"]
    project = next((p for p in projects if p["name"] == project_name), None)
    if not project:
        raise Exception(f"Project '{project_name}' not found in organization '{organization}'")
    print(f"##[debug] Found Project ID: {project['id']}")
    return project["id"]

def get_repository_id(organization: str, project_id: str, repository_name: str, token: str) -> str:
    """Get Azure DevOps repository ID."""
    print(f"##[debug] Looking up Repository ID for: {repository_name} in Project ID: {project_id}")
    uri = f"https://dev.azure.com/{organization}/{project_id}/_apis/git/repositories?api-version=7.0"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(uri, headers=headers, timeout=30)
    response.raise_for_status()
    repos = response.json()["value"]
    repo = next((r for r in repos if r["name"] == repository_name), None)
    if not repo:
        raise Exception(f"Repository '{repository_name}' not found in project ID '{project_id}'")
    print(f"##[debug] Found Repository ID: {repo['id']}")
    return repo["id"]

def initialize_git_configuration(token: str, organization: str, project: str, repository: str):
    """Configure git settings."""
    print("##[debug] Initializing Git configurations...")
    configs = [
        ["--global", "core.longpaths", "true"],
        ["--global", "core.autocrlf", "false"],
        ["--global", "core.packedGitLimit", "512m"],
        ["--global", "core.packedGitWindowSize", "512m"],
        ["--global", "pack.windowMemory", "512m"],
        ["--global", "pack.packSizeLimit", "512m"],
        ["--global", "http.postBuffer", "524288000"],
        ["--global", "user.email", "azure-pipeline@bhg.com"],
        ["--global", "user.name", "Azure Pipeline"]
    ]
    for config in configs:
        subprocess.run(["git", "config"] + config, check=True)

def new_release_branch(version: str, source_branch: str, organization: str, project_id: str, repository_id: str) -> Dict[str, str]:
    """Create a release branch in Azure DevOps."""
    print(f"##[section] Creating release branch for version {version} from {source_branch}")
    token = get_devops_auth_token()
    temp_dir = tempfile.mkdtemp(prefix="release_")
    print(f"##[debug] Working in temporary directory: {temp_dir}")
    repo_url = f"https://oauth2:{token}@dev.azure.com/{organization}/{project_id}/_git/{repository_id}"
    try:
        subprocess.run(["git", "clone", "-b", source_branch, "--single-branch", "--depth=1", repo_url, temp_dir], check=True)
        os.chdir(temp_dir)
        branch_name = version
        subprocess.run(["git", "checkout", "-b", branch_name], check=True)
        print(f"##[debug] Successfully created release branch: {branch_name}")
        return {"BranchName": branch_name, "TempDir": temp_dir}
    except subprocess.CalledProcessError as e:
        print(f"##[error] Failed to create release branch: {e}")
        raise

def invoke_az_devops_api(uri: str, token: str, method: str = "GET", body: Optional[Any] = None) -> Any:
    """Invoke Azure DevOps API."""
    print(f"##[debug] Invoking API: {uri}")
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    try:
        if method == "GET":
            response = requests.get(uri, headers=headers, timeout=30)
        else:
            response = requests.request(method, uri, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"##[error] API call failed: {e}")
        if hasattr(e, "response") and e.response:
            print(f"##[error] Status Code: {e.response.status_code}")
            try:
                print(f"##[error] Error Details: {e.response.text}")
            except:
                print("##[warning] Could not read error response body")
        return None

def get_completed_pull_requests(organization: str, project_id: str, repository_id: str, days_to_look_back: int, token: str) -> List[Dict]:
    """Get completed pull requests within the specified date range."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_to_look_back)
    print(f"##[debug] Getting completed pull requests between {start_date.strftime('%Y-%m-%d')} and {end_date.strftime('%Y-%m-%d')}")
    uri = (f"https://dev.azure.com/{organization}/{project_id}/_apis/git/repositories/{repository_id}/pullrequests?"
           f"searchCriteria.status=completed&searchCriteria.minTime={start_date.strftime('%Y-%m-%d')}&"
           f"searchCriteria.targetRefName=refs/heads/main&api-version=7.1")
    prs = invoke_az_devops_api(uri, token)
    if prs and prs.get("value"):
        filtered_prs = [
            pr for pr in prs["value"]
            if start_date <= datetime.strptime(pr["closedDate"], "%Y-%m-%dT%H:%M:%S.%fZ") <= end_date
        ]
        print(f"##[debug] Found {len(filtered_prs)} completed pull requests in date range")
        return filtered_prs
    print("##[debug] No pull requests found or API call failed")
    return []

def get_work_items_for_pull_request(organization: str, project_id: str, repository_id: str, pull_request_id: int, token: str) -> List[Dict]:
    """Get work items for a pull request."""
    uri = (f"https://dev.azure.com/{organization}/{project_id}/_apis/git/repositories/{repository_id}/"
           f"pullRequests/{pull_request_id}/workitems?api-version=7.1")
    work_items = invoke_az_devops_api(uri, token)
    if work_items and work_items.get("value"):
        print(f"##[debug] Found {len(work_items['value'])} work items for PR #{pull_request_id}")
        return work_items["value"]
    print(f"##[debug] No work items found for PR #{pull_request_id} or API call failed")
    return []

def get_work_item_details(organization: str, project_id: str, work_item_id: int, token: str) -> Dict:
    """Get work item details."""
    uri = (f"https://dev.azure.com/{organization}/{project_id}/_apis/wit/workitems/{work_item_id}?"
           f"$expand=all&api-version=7.1")
    return invoke_az_devops_api(uri, token)

def format_release_notes(pull_request_info: Dict, work_item_info: Dict) -> Dict:
    """Format release notes for a work item."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    work_item_type = work_item_info["fields"].get("System.WorkItemType", "")
    change_type = work_item_info["fields"].get("Custom.ChangeType", "")
    release_notes = work_item_info["fields"].get("Custom.ReleaseNotes", "")
    release_date = work_item_info["fields"].get("Custom.ReleaseDate", datetime.now().strftime("%Y-%m-%d"))
    return {
        "WorkItemId": work_item_info["id"],
        "WorkItemType": work_item_type,
        "Title": work_item_info["fields"].get("System.Title", ""),
        "CommitId": None,
        "CommitMessage": None,
        "CommitDate": None,
        "PullRequestId": pull_request_info["pullRequestId"],
        "PullRequestTitle": pull_request_info["title"],
        "ChangeType": change_type,
        "ReleaseNotes": release_notes,
        "DeployedVersion": args.ReleaseVersion,
        "ReleaseDate": release_date,
        "Timestamp": timestamp
    }

def export_release_notes_to_markdown(release_notes: List[Dict], output_path: str, organization: str, project: str) -> str:
    """Export release notes to markdown file."""
    markdown_content = f"""# Changelog

The following contains all major, minor, and patch version release notes.

{EMOJIS['bullet']} {EMOJIS['breaking']} Breaking change!
{EMOJIS['bullet']} {EMOJIS['feature']} New Functionality
{EMOJIS['bullet']} {EMOJIS['fix']} Bug Fix
{EMOJIS['bullet']} {EMOJIS['docs']} Documentation Update
{EMOJIS['bullet']} {EMOJIS['internal']} Internal Optimization
{EMOJIS['bullet']} {EMOJIS['unknown']} Unspecified Change Type

"""
    grouped_notes = {}
    for note in release_notes:
        version = note["DeployedVersion"]
        if version not in grouped_notes:
            grouped_notes[version] = []
        grouped_notes[version].append(note)

    for version in sorted(grouped_notes.keys(), reverse=True):
        notes = grouped_notes[version]
        release_date = notes[0]["ReleaseDate"]
        markdown_content += f"\n## Version {version}\n\nRelease Date: {release_date}\n\n"
        for note in notes:
            emoji = {
                "Breaking Change": EMOJIS["breaking"],
                "New Functionality": EMOJIS["feature"],
                "Bug Fix": EMOJIS["fix"],
                "Documentation Update": EMOJIS["docs"],
                "Internal Optimization": EMOJIS["internal"]
            }.get(note["ChangeType"], EMOJIS["unknown"])
            clean_notes = note["ReleaseNotes"] or "No details provided"
            clean_notes = re.sub(r'<div>|</div>|<ul>|</ul>|<li>|</li>|<br>|<[/]?(p|span|b|i|strong|em|h\d)[^>]*>|&quot;', '', clean_notes)
            clean_notes = clean_notes.replace('&', '&amp;').strip()
            clean_notes = re.sub(r'(\n\s*){2,}', '\n', clean_notes)
            clean_lines = [f"  * {line}" if line.strip() else line for line in clean_notes.split('\n')]
            clean_notes = '\n'.join(clean_lines)
            encoded_project = urllib.parse.quote(project)
            markdown_content += (f"{EMOJIS['bullet']} {emoji} {clean_notes} "
                                f"([#{note['WorkItemId']}](https://dev.azure.com/{organization}/{encoded_project}/_workitems/edit/{note['WorkItemId']}))\n\n")

    # Write with UTF-8 BOM
    with open(output_path, "w", encoding="utf-8-sig") as f:
        f.write(markdown_content)
    print(f"##[debug] Changelog exported to {output_path}")
    return markdown_content

def commit_changelog_to_release_branch(changelog_path: str, branch_name: str, temp_dir: str):
    """Commit and push changelog to release branch."""
    print("##[section] Committing changelog to release branch")
    os.chdir(temp_dir)
    subprocess.run(["git", "add", changelog_path], check=True)
    subprocess.run(["git", "commit", "-m", f"docs: Add changelog for release {branch_name}"], check=True)
    subprocess.run(["git", "push", "origin", branch_name], check=True)
    print(f"##[debug] Successfully committed and pushed changelog to {branch_name}")

def get_channel_info(teams_url: str) -> Dict:
    """Parse Teams URL to extract team and channel IDs."""
    print(f"##[debug] Parsing Teams URL: {teams_url}")
    channel_info = {"TeamId": None, "ChannelId": None, "IsValid": False, "ErrorMessage": "", "RawChannelId": None, "DecodedChannelId": None}
    try:
        uri = urllib.parse.urlparse(teams_url)
        query_params = urllib.parse.parse_qs(uri.query)
        team_id = query_params.get("groupId", [None])[0]
        if team_id:
            channel_info["TeamId"] = team_id
        match = re.search(r"/l/channel/([^/]+)/", uri.path)
        if match:
            raw_channel_id = match.group(1)
            channel_info["RawChannelId"] = raw_channel_id
            channel_info["DecodedChannelId"] = urllib.parse.unquote(raw_channel_id)
            channel_info["ChannelId"] = channel_info["DecodedChannelId"]
        channel_info["IsValid"] = bool(
            team_id and re.match(r"^[0-9a-fA-F-]{36}$", team_id) and
            channel_info["ChannelId"] and re.match(r"^19:.*@thread", channel_info["ChannelId"])
        )
        if not channel_info["IsValid"]:
            errors = []
            if not team_id:
                errors.append("Team ID not found")
            elif not re.match(r"^[0-9a-fA-F-]{36}$", team_id):
                errors.append(f"Team ID format invalid: {team_id}")
            if not channel_info["ChannelId"]:
                errors.append("Channel ID not found")
            elif not re.match(r"^19:.*@thread", channel_info["ChannelId"]):
                errors.append(f"Channel ID format invalid: {channel_info['ChannelId']}")
            channel_info["ErrorMessage"] = "; ".join(errors)
        print(f"##[debug] Final Team ID: {channel_info['TeamId']}")
        print(f"##[debug] Final Channel ID: {channel_info['ChannelId']}")
        print(f"##[debug] Validation Result: {channel_info['IsValid']}")
        if not channel_info["IsValid"]:
            print(f"##[debug] Validation Errors: {channel_info['ErrorMessage']}")
        return channel_info
    except Exception as e:
        print(f"##[warning] Error parsing Teams URL: {e}")
        channel_info["ErrorMessage"] = f"Exception: {e}"
        return channel_info

def convert_markdown_links_to_html(text: str) -> str:
    """Convert Markdown links to HTML links."""
    print(f"##[debug] Converting markdown links to HTML")
    try:
        def replace_link(match):
            link_text, link_url = match.groups()
            html_link = f'<a href="{link_url}">{link_text}</a>'
            print(f"##[debug] Converted link: [{link_text}]({link_url}) -> {html_link}")
            return html_link
        return re.sub(r'\[([^\[\]]+)\]\(([^()]+)\)', replace_link, text)
    except Exception as e:
        print(f"##[warning] Error converting markdown links: {e}")
        return text

def format_markdown_for_teams(markdown_content: str, repository: str, release_version: str) -> str:
    """Format markdown content for Teams message."""
    print("##[debug] Formatting markdown content for Teams")
    try:
        teams_header = (f'<p><strong>{TEAMS_EMOJIS["rocket"]} Release Notification</strong></p>'
                        f'<p><strong>Repository:</strong> {repository}<br/>'
                        f'<strong>Version:</strong> {release_version}<br/>'
                        f'<strong>Release Date:</strong> {datetime.now().strftime("%Y-%m-%d")}</p>'
                        f'<p><strong>Changelog</strong></p>'
                        f'<p>The following contains all major, minor, and patch version release notes.</p>'
                        f'<ul>'
                        f'<li> {TEAMS_EMOJIS["breaking"]} Breaking change!</li>'
                        f'<li> {TEAMS_EMOJIS["feature"]} New Functionality</li>'
                        f'<li> {TEAMS_EMOJIS["fix"]} Bug Fix</li>'
                        f'<li> {TEAMS_EMOJIS["docs"]} Documentation Update</li>'
                        f'<li> {TEAMS_EMOJIS["internal"]} Internal Optimization</li>'
                        f'<li> {TEAMS_EMOJIS["unknown"]} Unspecified Change Type</li>'
                        f'</ul>')
        version_content = ""
        in_version_section = False
        lines = markdown_content.splitlines()
        for line in lines:
            if match := re.match(r'^## Version (.+)$', line):
                version_content += f"<h3>Version {match.group(1)}</h3>\n"
                in_version_section = True
                continue
            if in_version_section and (match := re.match(r'^Release Date: (.+)$', line)):
                version_content += f"<p>Release Date: {match.group(1)}</p>\n"
                continue
            if in_version_section and (match := re.match(rf'^{EMOJIS["bullet"]} (.+)$', line)):
                note_content = match.group(1)
                for emoji_name, unicode in EMOJIS.items():
                    if emoji_name in ["breaking", "feature", "fix", "docs", "internal", "unknown"]:
                        note_content = note_content.replace(unicode, TEAMS_EMOJIS[emoji_name])
                note_content = convert_markdown_links_to_html(note_content)
                note_content = note_content.replace('&', '&amp;')
                version_content += f"<p>{note_content}</p>\n"
                continue
            if in_version_section and line.strip() and not line.startswith('#'):
                clean_line = convert_markdown_links_to_html(line.strip())
                clean_line = clean_line.replace('&', '&amp;')
                version_content += f"<p>{clean_line}</p>\n"
        if not version_content:
            version_content = (f"<h3>Version {release_version}</h3>\n"
                              f"<p>Release Date: {datetime.now().strftime('%Y-%m-%d')}</p>\n"
                              f"<p>No release notes available for this version.</p>\n")
        final_content = teams_header + version_content
        print(f"##[debug] Final Teams content length: {len(final_content)} characters")
        return final_content
    except Exception as e:
        print(f"##[warning] Error formatting markdown for Teams: {e}")
        return (f'<p><strong>{TEAMS_EMOJIS["rocket"]} Release Notification</strong></p>'
                f'<p>Repository: {repository}<br/>Version: {release_version}</p>'
                f'<p>{markdown_content}</p><hr/>'
                f'<p><em>{TEAMS_EMOJIS["robot"]} Automated release notification from Azure DevOps</em></p>')

def get_teams_channel_tags(team_id: str, token: str) -> List[Dict]:
    """Get tags for a Teams team."""
    print(f"##[debug] Getting tags for Team: {team_id}")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    tags_url = f"https://graph.microsoft.com/v1.0/teams/{team_id}/tags"
    try:
        response = requests.get(tags_url, headers=headers, timeout=30)
        response.raise_for_status()
        tags = response.json().get("value", [])
        print(f"##[info] Found {len(tags)} tags in the team")
        return [{"id": tag["id"], "displayName": tag["displayName"], "description": tag.get("description", ""),
                 "memberCount": tag.get("memberCount", 0), "tagType": tag.get("tagType", ""), "teamId": team_id}
                for tag in tags]
    except requests.RequestException as e:
        print(f"##[error] Failed to get team tags: {e}")
        if hasattr(e, "response") and e.response:
            print(f"##[error] HTTP Status Code: {e.response.status_code}")
            if e.response.status_code == 404:
                print("##[warning] Tags may not be available for this team or you may lack permissions")
            try:
                print(f"##[error] Response body: {e.response.text}")
            except:
                print("##[warning] Could not read error response body")
        return []

def send_teams_channel_message(team_id: str, channel_id: str, markdown_content: str, repository: str, release_version: str, token: str) -> bool:
    """Send release notes to Teams channel."""
    print("##[section] Sending Teams notification")
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"}
    available_tags = get_teams_channel_tags(team_id, token)
    if not available_tags:
        print("##[warning] No tags found in this team")
    tags_to_mention = [
        tag for tag in available_tags
        if tag["displayName"] in (os.getenv("TEAMS_TAGS", "").split(",") if os.getenv("TEAMS_TAGS") else [])
    ]
    print(f"##[debug] Available tags: {json.dumps(available_tags, indent=2)}")
    formatted_content = format_markdown_for_teams(markdown_content, repository, release_version)
    mentions = []
    tag_mentions_notification = []
    for i, tag in enumerate(tags_to_mention):
        mentions.append({
            "id": i,
            "mentionText": tag["displayName"],
            "mentioned": {"tag": {"id": tag["id"], "displayName": tag["displayName"]}}
        })
        tag_mentions_notification.append(f'<at id="{i}">{tag["displayName"]}</at>')
    cc_text = ", ".join(tag_mentions_notification) if tag_mentions_notification else "No tags available"
    cc_footer = (f'<hr/>\n<p><strong>{TEAMS_EMOJIS["cc"]} CC (Notifications):</strong> {cc_text}</p>\n'
                 f'<hr/>\n<p><em>{TEAMS_EMOJIS["robot"]} Automated release notification from Azure DevOps</em></p>')
    formatted_content += cc_footer
    escaped_content = formatted_content.replace('"', '\\"')
    body = {
        "subject": f"Release Notification - {release_version}",
        "importance": "high",
        "body": {"contentType": "html", "content": escaped_content}
    }
    if mentions:
        body["mentions"] = mentions
    message_url = f"https://graph.microsoft.com/beta/teams/{team_id}/channels/{channel_id}/messages"
    print(f"##[debug] Sending message to Teams channel: {message_url}")
    print(f"##[debug] Message payload: {json.dumps(body, indent=2)}")
    print(f"##[debug] Message length: {len(formatted_content)} characters")
    print(f"##[debug] Repository: {repository}")
    print(f"##[debug] Version: {release_version}")
    print(f"##[debug] Formatted content preview: {formatted_content[:300]}...")
    try:
        response = requests.post(message_url, headers=headers, json=body, timeout=30)
        response.raise_for_status()
        print(f"Message sent successfully to Teams channel with HTML entities")
        print(f"##[debug] Message ID: {response.json()['id']}")
        return True
    except requests.RequestException as e:
        print(f"##[error] Failed to send Teams message: {e}")
        if hasattr(e, "response") and e.response:
            print(f"##[error] HTTP Status Code: {e.response.status_code}")
            try:
                print(f"##[error] Response body: {e.response.text}")
            except:
                print("##[warning] Could not read error response body")
        return False

def main():
    """Main execution flow."""
    try:
        version = args.ReleaseVersion
        project = args.Project.strip()
        repository = args.Repository.strip()
        print(f"##[section] {EMOJIS['gear']} Validating environment variables")
        token = get_devops_auth_token()
        test_git_repository()
        initialize_git_configuration(token, args.Organization, project, repository)
        print("##[section] Looking up Project and Repository IDs")
        project_id = get_project_id(args.Organization, project, token)
        repository_id = get_repository_id(args.Organization, project_id, repository, token)
        branch_info = new_release_branch(version, args.SourceBranch, args.Organization, project_id, repository_id)
        branch_name = branch_info["BranchName"]
        temp_dir = branch_info["TempDir"]
        print(f"##[section] Generating changelog for release {version}")
        pull_requests = get_completed_pull_requests(args.Organization, project_id, repository_id, args.DaysToLookBack, token)
        release_notes = []
        for pr in pull_requests:
            print(f"##[debug] Processing pull request #{pr['pullRequestId']}: {pr['title']}")
            work_items = get_work_items_for_pull_request(args.Organization, project_id, repository_id, pr["pullRequestId"], token)
            for work_item in work_items:
                work_item_details = get_work_item_details(args.Organization, project_id, work_item["id"], token)
                if (work_item_details and work_item_details.get("fields") and
                        work_item_details["fields"].get("System.WorkItemType") == "Product Backlog Item"):
                    print(f"##[debug]    Found Product Backlog Item #{work_item_details['id']}: {work_item_details['fields']['System.Title']}")
                    release_notes.append(format_release_notes(pr, work_item_details))
                elif (work_item_details and work_item_details.get("fields") and
                      work_item_details["fields"].get("System.WorkItemType") == "Bug" and
                      work_item_details["fields"].get("System.State") in ["Resolved", "Done"]):
                    release_notes.append(format_release_notes(pr, work_item_details))
                elif work_item_details:
                    print(f"##[debug]    Skipping work item #{work_item_details['id']}: not a Product Backlog Item (type: {work_item_details['fields']['System.WorkItemType']})")
        changelog_path = os.path.join(temp_dir, "changelog.md")
        if release_notes:
            print(f"##[section] Found {len(release_notes)} release notes entries")
            markdown_content = export_release_notes_to_markdown(release_notes, changelog_path, args.Organization, project)
            commit_changelog_to_release_branch(changelog_path, branch_name, temp_dir)
            if args.SendTeamsNotification and args.TeamsChannelWebUrl:
                print(f"##[section] {EMOJIS['bell']} Sending Teams notification")
                teams_token = get_teams_token()
                channel_info = get_channel_info(args.TeamsChannelWebUrl)
                if channel_info["IsValid"]:
                    send_teams_channel_message(
                        channel_info["TeamId"], channel_info["ChannelId"], markdown_content,
                        repository, version, teams_token
                    )
                else:
                    print(f"##[error] Invalid Teams channel info: {channel_info['ErrorMessage']}")
            else:
                print(f"##[debug] {EMOJIS['info']} Teams notification is disabled or URL not provided")
        else:
            print(f"##[section] {EMOJIS['warning']} No release notes found, creating empty changelog")
            empty_changelog = (f"# Changelog\n\n"
                              f"The following contains all major, minor, and patch version release notes.\n\n"
                              f"- {EMOJIS['breaking']} Breaking change!\n"
                              f"- {EMOJIS['feature']} New Functionality\n"
                              f"- {EMOJIS['fix']} Bug Fix\n"
                              f"- {EMOJIS['docs']} Documentation Update\n"
                              f"- {EMOJIS['internal']} Internal Optimization\n\n"
                              f"## Version {version}\n"
                              f"Release Date: {datetime.now().strftime('%Y-%m-%d')}\n\n"
                              f"No release notes available for this version.")
            with open(changelog_path, "w", encoding="utf-8-sig") as f:
                f.write(empty_changelog)
            commit_changelog_to_release_branch(changelog_path, branch_name, temp_dir)
        print(f"##vso[task.setvariable variable=ReleaseBranchName;isoutput=true]{branch_name}")
        print(f"##vso[task.setvariable variable=ReleaseVersion;isoutput=true]{version}")
        print(f"##vso[task.setvariable variable=ReleaseNotesCount;isoutput=true]{len(release_notes)}")
        print(f"##[section] {EMOJIS['rocket']} Release branch creation completed successfully")
        print(f"{EMOJIS['check']} Release Branch: {branch_name}")
        print(f"{EMOJIS['check']} Version: {version}")
        print(f"{EMOJIS['check']} Release Notes Count: {len(release_notes)}")
        print(f"{EMOJIS['check']} Teams Notification: {'Enabled' if args.SendTeamsNotification else 'Disabled'}")
    except Exception as e:
        print(f"##[error] {e}")
        exit(1)
    finally:
        git_credentials = os.path.expanduser("~/.git-credentials")
        if os.path.exists(git_credentials):
            os.remove(git_credentials)

if __name__ == "__main__":
    main()
