if ($args[0] -eq "AWS")
{
    $dataRootDirectory=$args[1]+"/metadata/datasets"
    $items=Get-ChildItem -Path ($dataRootDirectory + "/*/*.json") -Force
    $queryPath= $args[1]+"/queries/datasets"
}else
{
    $dataRootDirectory = Join-Path $args[1] $args[2] ingestion datasets
    $items = Get-ChildItem -File -Recurse "$dataRootDirectory" -Filter "*.json"
    $queryPath = Join-Path $args[1] queries datasets
}
foreach($item in $items)
{
  $json=(Get-Content -Raw $item.FullName) | ConvertFrom-Json

  if ($json.datasetTypeName -eq "database")
  {
    $fileName = $json.datasetSchema + "_" + $json.datasetName + ".json"

    $outputDirectory=Split-Path -Path ($item.FullName)
    New-Item (Join-Path $outputDirectory watermark) -ItemType "directory" -Force
    if($json.sourceSystemProperties.includeSpecificColumns -eq $null)
    {
      $columnList="*"
    }
    else
    {
      $columnList=($json.sourceSystemProperties.includeSpecificColumns -join ",")
    }
    switch ($json.sourceSystemProperties.ingestType)
        {
          "full"
          {
            $query=("""SELECT " + $columnList + " FROM " + $json.datasetSchema + "." + $json.datasetName + """")
            $outputJson="{ ""query"": " + $query + " }"
            ;break
          }
          "watermark"
          {
            if ($json.sourceSystemProperties.isDynamicQuery -eq $false)
            {
              $query=("""SELECT " + $columnList + " FROM " + $json.datasetSchema + "." + $json.datasetName + " " + $json.sourceSystemProperties.filterExpression + """")
              $outputJson="{ ""query"": " + $query + " }"
            }
            else
            {
              if (($columnList -ne "*") -and (-not($json.sourceSystemProperties.includeSpecificColumns -contains $json.sourceSystemProperties.sourceEntityWatermarkIdentifier)))
              {
                $columnList=$columnList + "," + $json.sourceSystemProperties.sourceEntityWatermarkIdentifier
              }

              $containsIdentifier=($json.sourceSystemProperties.includeSpecificColumns -contains $json.sourceSystemProperties.sourceEntityWatermarkIdentifier)
              $substitutedFilePath = ($item.FullName -replace [regex]::Escape($dataRootDirectory), $queryPath)

              $existingQueryFile = Join-Path (Split-Path -Path ($substitutedFilePath)) watermark $fileName

              if($json.sourceSystemProperties.filterExpression -ne $null)
              {
                $filterExpression=" " + $json.sourceSystemProperties.filterExpression
              }
              else
              {
                $filterExpression=""
              }

              $watermarkValue="1900-01-01T00:00:00Z"

              if (Test-Path $existingQueryFile -PathType Leaf)
              {
                $existingContent=(Get-Content -Raw $existingQueryFile) | ConvertFrom-Json
                if (($existingContent.sourceEntityWatermarkIdentifier -eq $json.sourceSystemProperties.sourceEntityWatermarkIdentifier) -and
                    ($existingContent.watermarkValue -ne $null))
                {
                  $watermarkValue=$existingContent.watermarkValue

                  if($watermarkValue -is [DateTime])
                  {
                    $watermarkValue=$watermarkValue.ToString('o')
                  }
                }
              }
              $query=("""SELECT " + $columnList + " FROM " + $json.datasetSchema + "." + $json.datasetName + " WHERE " + $json.sourceSystemProperties.sourceEntityWatermarkIdentifier + " > CAST('" + $watermarkValue + "' as datetime2)" + $filterExpression + """")
              $outputJson="{ ""query"": " + $query + ",""sourceEntityWatermarkIdentifier"": """ + $json.sourceSystemProperties.sourceEntityWatermarkIdentifier + """,""watermarkValue"": """ + $watermarkValue + """ }"
            }

            ;break
          }
          "cdc"
          {
               if($json.prepProperties.primaryKeyList -eq $null)
                {
                  $primaryKeyList="*"
                }
                else
                {
                  $primaryKeyList=($json.prepProperties.primaryKeyList -join ",")
                }
            if ($json.sourceSystemProperties.isDynamicQuery -eq $false)
            {
              $query=("""SELECT " + $columnList + " FROM cdc."+$json.datasetSchema + "_" + $json.datasetName + "_CT " + $json.sourceSystemProperties.filterExpression + """")
              $outputJson="{ ""query"": " + $query + " }"
            }
            else
            {
              if (($columnList -ne "*") -and (-not($json.sourceSystemProperties.includeSpecificColumns -contains $json.sourceSystemProperties.sourceEntityWatermarkIdentifier)))
              {
                $columnList=$columnList + "," + $json.sourceSystemProperties.sourceEntityWatermarkIdentifier
              }

              $containsIdentifier=($json.sourceSystemProperties.includeSpecificColumns -contains $json.sourceSystemProperties.sourceEntityWatermarkIdentifier)
              $substitutedFilePath = ($item.FullName -replace [regex]::Escape($dataRootDirectory), $queryPath)

              $existingQueryFile = Join-Path (Split-Path -Path ($substitutedFilePath)) watermark $fileName

              if($json.sourceSystemProperties.filterExpression -ne $null)
              {
                $filterExpression=" " + $json.sourceSystemProperties.filterExpression
              }
              else
              {
                $filterExpression=""
              }

              $watermarkValue="1900-01-01T00:00:00.000"

              if (Test-Path $existingQueryFile -PathType Leaf)
              {
                $existingContent=(Get-Content -Raw $existingQueryFile) | ConvertFrom-Json
                if (($existingContent.sourceEntityWatermarkIdentifier -eq $json.sourceSystemProperties.sourceEntityWatermarkIdentifier) -and
                    ($existingContent.watermarkValue -ne $null))
                {
                  $watermarkValue=$existingContent.watermarkValue
                }
              }
              $query=("""WITH CDC AS (SELECT ROW_NUMBER() OVER( PARTITION BY " +$primaryKeyList+ " ORDER BY " +$json.sourceSystemProperties.sourceEntityWatermarkIdentifier+ " DESC) AS Row_Num, "+ $columnList + " FROM cdc."+ $json.datasetSchema + "_" + $json.datasetName + "_CT  WHERE " +$json.sourceSystemProperties.operationColumn+ " <> 3) SELECT * FROM CDC WHERE Row_Num = 1 AND " + $json.sourceSystemProperties.sourceEntityWatermarkIdentifier + " >  sys.fn_cdc_map_time_to_lsn('smallest greater than','"+ $watermarkValue +"') " + $filterExpression + """")
              $outputJson="{ ""query"": " + $query + ",""sourceEntityWatermarkIdentifier"": """ + $json.sourceSystemProperties.sourceEntityWatermarkIdentifier + """,""watermarkValue"": """ + $watermarkValue + """ }"
            }

            ;break
          }
          Default
          {
            $query=("""select " + $columnList + " from " + $json.datasetSchema + "." + $json.datasetName + """")
            $outputJson="{ ""query"": " + $query + " }"
            ;break
         }
        }

    Write-Output $outputJson

    New-Item (Join-Path $outputDirectory watermark $fileName) -Force
    Set-Content (Join-Path $outputDirectory watermark $fileName) $outputJson -NoNewline
  }
}