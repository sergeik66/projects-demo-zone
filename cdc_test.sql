WITH LatestChanges AS (
    SELECT 
        [CODE],
        [PRIMESEQ],
        [STATE],
        [EFFDATE],
        [CLASSCODE],
        [SEQCODE],
        [TERRITORY],
        [EXMOD],
        [MINPREM],
        [EXPOSURE],
        [PREMIUM],
        [RATE],
        [XCLUAMT],
        [LOSSCONS],
        [FORM],
        [PARAGRAPH],
        [VOLCOMP],
        [HAZARD],
        [CVRG],
        [DISCTYPE],
        [CLASSUFFIX],
        [DIFFPREM],
        [DIFFRATE],
        [rateEFFDATE],
        [LOSSCOST],
        CASE WHEN [__$operation] = 1 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS is_deleted,
        ROW_NUMBER() OVER (
            PARTITION BY 
                [CLASSCODE], 
                [CODE], 
                [EFFDATE], 
                [PRIMESEQ], 
                [rateEFFDATE], 
                [SEQCODE], 
                [STATE], 
                [TERRITORY], 
                [VOLCOMP]
            ORDER BY [__$seqval] DESC
        ) AS rn
    FROM [cdc].[dbo_wcpayrol_CT]
    WHERE [__$operation] IN (1, 2, 4) -- Include insert, update (after image), and delete
)
SELECT 
    [CODE],
    [PRIMESEQ],
    [STATE],
    [EFFDATE],
    [CLASSCODE],
    [SEQCODE],
    [TERRITORY],
    [EXMOD],
    [MINPREM],
    [EXPOSURE],
    [PREMIUM],
    [RATE],
    [XCLUAMT],
    [LOSSCONS],
    [FORM],
    [PARAGRAPH],
    [VOLCOMP],
    [HAZARD],
    [CVRG],
    [DISCTYPE],
    [CLASSUFFIX],
    [DIFFPREM],
    [DIFFRATE],
    [rateEFFDATE],
    [LOSSCOST],
    is_deleted
FROM LatestChanges
WHERE rn = 1;
