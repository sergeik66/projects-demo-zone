README: Database Restore Status Check PipelineOverviewThis Azure Data Factory (ADF) pipeline checks the restore status of a dynamic list of SQL Server databases to determine if all have been restored on the current date and are in an ONLINE state. It uses a Lookup activity to execute a SQL query, an Until activity with a Wait activity to poll until the restore is complete, and sets a pipeline variable (RestoreStatus) to return the result to the caller (e.g., a parent pipeline). The pipeline is designed for monitoring database restore operations in a SQL Server environment.PurposeThe pipeline verifies that all specified databases:Have at least one restore history entry in msdb.dbo.restorehistory for the current date (YYYY-MM-DD).
Are in state = 1 (ONLINE) in sys.databases.
It returns RestoreStatus = true only if all databases meet these conditions, otherwise false.

PrerequisitesAzure Data Factory: An ADF instance with permissions to create and run pipelines.
SQL Server: Access to a SQL Server instance with:msdb.dbo.restorehistory and sys.databases system tables.
SQL Server 2016 or later (for STRING_SPLIT function).
A user with read permissions on msdb and master databases.

Linked Service: A configured ADF linked service to the SQL Server instance.
Test Data: Sample restore history data for testing (optional for development).

Pipeline ComponentsLookup Activity: Executes a dynamic SQL query to check if all specified databases have restore history for the current date and are ONLINE.
Until Activity: Loops until the restore status is complete (restoredCompleted = true) or a timeout/max iterations is reached.
Wait Activity: Introduces a configurable delay between iterations inside the Until activity.
Set Variable Activity: Sets the RestoreStatus variable (boolean) based on the final restore status.
Output: Returns RestoreStatus to the caller via the pipeline’s output.

ParametersThe pipeline uses the following parameters, configurable at runtime:Parameter Name
Type
Description
Default Value
Example
DatabaseNames
String
Comma-separated, single-quoted list of database names to check (e.g., 'demo','demo1').
None (required)
'demo','demo1','demo2'
WaitSeconds
Int
Delay (in seconds) between Until iterations.
30
60
MaxIterations
Int
Maximum number of Until iterations to prevent infinite loops.
100
200
TimeoutSeconds
Int
Maximum pipeline runtime (in seconds) for Until activity.
3600 (1 hour)
7200

VariablesVariable Name
Type
Description
RestoreStatus
Boolean
Stores the final restore status (true if all databases are restored and ONLINE, false otherwise).

ConfigurationFollow these steps to configure the pipeline:Import Pipeline:Import the pipeline JSON into your ADF instance using the ADF UI (Azure Portal or VS Code with ADF extension).
Alternatively, deploy via Git integration if configured.

Set Up Linked Service:Ensure a SQL Server linked service is configured in ADF:Name: e.g., SqlServerLinkedService.
Connection: Provide server name, database (msdb), and authentication (e.g., SQL Server Authentication or Managed Identity).
Test the connection in ADF to confirm access to msdb.dbo.restorehistory and sys.databases.

Configure Parameters:In the ADF pipeline editor, set default values for parameters or pass them at runtime:DatabaseNames: Specify the list of databases (e.g., 'demo','demo1').
WaitSeconds: Set delay between checks (e.g., 30 seconds).
MaxIterations: Set max loops (e.g., 100).
TimeoutSeconds: Set max runtime (e.g., 3600 for 1 hour).

Use the ADF UI’s “Parameters” tab or pass values from a parent pipeline.

Validate SQL Access:Ensure the SQL Server user has SELECT permissions on:msdb.dbo.restorehistory
sys.databases in the master database.

Test the SQL query manually in SSMS/Azure Data Studio to confirm it returns restoredCompleted as a boolean.

Set Variable Output:The pipeline automatically sets RestoreStatus based on the Lookup activity’s output.
No additional configuration is needed unless the caller requires a custom output format.

UsageRun Pipeline:Manually: Trigger the pipeline via ADF UI’s “Debug” or “Trigger Now” options, providing parameter values.
From Parent Pipeline: Call the pipeline using an Execute Pipeline activity, passing parameters and capturing RestoreStatus from the output (e.g., @activity('PipelineName').output.variables.RestoreStatus).
Scheduled: Add the pipeline to a trigger (e.g., Schedule Trigger) in ADF.

Monitor Execution:Use ADF’s “Monitor” tab to view pipeline runs, activity status, and errors.
Check the pipeline output for RestoreStatus value.

Retrieve Output:The caller (e.g., parent pipeline) can access RestoreStatus via:json

@activity('DatabaseRestoreCheckPipeline').output.variables.RestoreStatus

Use this value for conditional logic in the parent pipeline (e.g., proceed if true, fail if false).

Acceptance CriteriaThe pipeline is considered complete when it meets the following criteria:Functional:Accepts a dynamic DatabaseNames parameter (e.g., 'demo','demo1' or 'demo').
Lookup executes the SQL query, correctly passing DatabaseNames.
Until loops until restoredCompleted = true or timeout/max iterations.
Wait applies the configured delay (WaitSeconds).
Sets RestoreStatus to match restoredCompleted.
RestoreStatus is accessible to the caller via pipeline output.

Correctness:RestoreStatus = true only when all databases in DatabaseNames:Have restore history in msdb.dbo.restorehistory for the current date.
Are in sys.databases with state = 1 (ONLINE).

RestoreStatus = false if any database lacks restore history or is not ONLINE.
Works for 1 or more databases.

Error Handling:Logs SQL errors from Lookup and sets RestoreStatus = false.
Exits gracefully on Until timeout/max iterations, setting RestoreStatus = false.
Handles invalid DatabaseNames (e.g., empty or malformed) with logged error and RestoreStatus = false.

Performance:SQL query executes within 5 seconds for typical datasets (<100 restore history rows).
Pipeline respects TimeoutSeconds and MaxIterations.

Usability:Parameters and RestoreStatus are documented in the pipeline (annotations) and this README.
Pipeline JSON is exportable for deployment.

Testing:Passes tests for:All databases restored and ONLINE (RestoreStatus = true).
Some databases not restored or not ONLINE (RestoreStatus = false).
No restore history (RestoreStatus = false).
Single database in DatabaseNames.

RestoreStatus is verified as accessible to caller.

TestingSetup:Ensure test databases (e.g., demo, demo1) exist in SQL Server.
Create sample restore history in msdb.dbo.restorehistory for the current date.
Set some databases to ONLINE (state = 1) and others to RESTORING (state = 3) for negative testing.

Test Scenarios:Scenario 1: All databases restored and ONLINE → Verify RestoreStatus = true.
Scenario 2: One or more databases not restored → Verify RestoreStatus = false.
Scenario 3: One or more databases not ONLINE → Verify RestoreStatus = false.
Scenario 4: Single database in DatabaseNames → Verify correct RestoreStatus.
Scenario 5: Invalid DatabaseNames (e.g., '') → Verify error logged, RestoreStatus = false.
Scenario 6: Timeout reached → Verify RestoreStatus = false.

Validation:Use ADF’s Debug mode to run the pipeline and check outputs.
Verify RestoreStatus in the pipeline output or parent pipeline.
Check ADF Monitor for errors or unexpected behavior.

TroubleshootingSQL Connection Errors:Verify linked service credentials and SQL Server accessibility.
Check user permissions on msdb and master databases.

Invalid DatabaseNames:Ensure format is correct (e.g., 'demo','demo1' with single quotes and commas).
Test with a single database to isolate issues.

Until Loop Issues:Check MaxIterations and TimeoutSeconds for appropriate values.
Ensure Wait delay is sufficient for SQL Server to update restore status.

RestoreStatus Not Returned:Verify Set Variable activity is correctly linked to Lookup output.
Check parent pipeline’s syntax for accessing RestoreStatus.

DeploymentExport Pipeline:Export the pipeline JSON from ADF UI or sync with Git repository.

Deploy to Higher Environment:Import JSON into target ADF instance (e.g., Test or Production).
Update linked service connection strings if different in the target environment.

Validate:Run pipeline in the target environment with test parameters.
Confirm RestoreStatus is returned correctly to the caller.

MaintenanceUpdates:Modify DatabaseNames parameter as new databases are added.
Adjust WaitSeconds or TimeoutSeconds based on restore performance.

Monitoring:Regularly check ADF Monitor for pipeline failures or timeouts.
Review SQL Server logs if restore history is missing.

Versioning:Store pipeline JSON in a Git repository for version control.
Document changes to parameters or logic in this README.

LimitationsRequires SQL Server 2016+ for STRING_SPLIT. For older versions, a custom string-splitting function is needed (contact the developer for assistance).
Assumes restore history is updated in real-time; delays in msdb updates may require longer WaitSeconds.
Does not include advanced error notifications (e.g., email alerts); add if needed.

ContactFor support or enhancements, contact the ADF administrator or the pipeline developer.

