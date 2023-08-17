USE COVID;

SELECT *
FROM DBO.Covid19;

-- 1. Retrieve the jurisdiction residence with the highest number of COVID deaths for the latest data period end date.

SELECT [Jurisdiction_Residence]
FROM Covid19
WHERE [data_period_end] = (SELECT MAX([data_period_end]) FROM Covid19)
ORDER BY [COVID_deaths] DESC;


-- 2. Calculate the week-over-week percentage change in crude COVID rate for all jurisdictions and groups, 
-- sorted by the highest percentage change first.

SELECT Jurisdiction_Residence, Grp, SUM(crude_COVID_rate) AS pct
FROM Covid19
GROUP BY Jurisdiction_Residence, Grp
ORDER BY pct DESC;

-- 3. Retrieve the top 5 jurisdictions with the highest percentage difference in aa_COVID_rate compared to the overall crude COVID rate 
-- for the latest data period end date.

SELECT TOP 5 Jurisdiction_Residence, data_period_end, crude_COVID_rate, aa_COVID_rate, (crude_COVID_rate - aa_COVID_rate) AS Diff
FROM Covid19
WHERE [data_period_end] = (SELECT MAX([data_period_end]) FROM Covid19)
ORDER BY Diff DESC;

-- 4. Calculate the average COVID deaths per week for each jurisdiction residence and group, for the latest 4 data period end dates.

SELECT TOP 4 Jurisdiction_Residence, GRP, SUM(covid_deaths) / COUNT(DISTINCT data_period_end) AS average_deaths_per_week
FROM Covid19
GROUP BY Jurisdiction_Residence, GRP, data_period_end
ORDER BY data_period_end DESC;


-- 5. Retrieve the data for the latest data period end date, but exclude any jurisdictions that had zero COVID deaths and have missing values 
-- in any other column.

SELECT *
FROM Covid19
WHERE data_period_end = (
  SELECT MAX(data_period_end)
  FROM Covid19
)
AND covid_deaths > 0
AND pct_change_wk IS NOT NULL
AND jurisdiction_residence IS NOT NULL
AND GRP IS NOT NULL
AND pct_diff_wk IS NOT NULL
AND crude_COVID_rate IS NOT NULL
AND aa_COVID_rate IS NOT NULL;


-- 6. Calculate the week-over-week percentage change in COVID_pct_of_total for all jurisdictions and groups, but only for the 
-- data period start dates after March 1, 2020.

SELECT Jurisdiction_Residence, Grp, data_period_start, SUM(COVID_pct_of_total) AS Total
FROM Covid19
WHERE data_period_start > '2020-03-01'
GROUP BY Jurisdiction_Residence, Grp, data_period_start
ORDER BY Total DESC;


-- 7. Group the data by jurisdiction residence and calculate the cumulative COVID deaths for each jurisdiction, but only up to the latest 
-- data period end date.

SELECT Jurisdiction_Residence, data_period_end, SUM(covid_deaths) AS cumulative_covid_deaths
FROM Covid19
WHERE data_period_end <= (
  SELECT MAX(data_period_end)
  FROM Covid19
)
GROUP BY Jurisdiction_Residence, data_period_end;

-- 8. Identify the jurisdiction with the highest percentage increase in COVID deaths from the previous week, and provide the actual numbers of deaths 
-- for each week. This would require a subquery to calculate the previous week's deaths.


SELECT TOP 1 Jurisdiction_Residence,
       data_period_end AS current_week_end,
       COVID_deaths AS current_week_deaths,
       LAG(COVID_deaths) OVER (PARTITION BY Jurisdiction_Residence ORDER BY data_period_end) AS previous_week_deaths,
CASE
	WHEN LAG(COVID_deaths) OVER (PARTITION BY Jurisdiction_Residence ORDER BY data_period_end) = 0
    THEN NULL
	ELSE ((COVID_deaths - LAG(COVID_deaths) OVER (PARTITION BY Jurisdiction_Residence ORDER BY data_period_end)) / NULLIF(LAG(COVID_deaths) 
	OVER (PARTITION BY Jurisdiction_Residence ORDER BY data_period_end), 0)) * 100
	END AS percentage_increase
FROM Covid19
WHERE data_period_end >= DATEADD(WEEK, -1, (SELECT MAX(data_period_end) FROM Covid19))
ORDER BY percentage_increase DESC;


-- 9. Compare the crude COVID death rates for different age groups, but only for jurisdictions where the total number of deaths exceeds 
-- a certain threshold (e.g. 100).

SELECT Jurisdiction_Residence, Grp, crude_COVID_rate
FROM Covid19
WHERE COVID_deaths > 100
ORDER BY crude_COVID_rate DESC;


-- 10. Implementation of Function & Procedure-"Create a stored procedure that takes in a date range and calculates the average weekly percentage change 
-- in COVID deaths for each jurisdiction. The procedure should return the average weekly percentage change along with the jurisdiction and date range 
-- as output. Additionally, create a user-defined function that takes in a jurisdiction as input and returns the average crude COVID rate for that 
-- jurisdiction over the entire dataset. Use both the stored procedure and the user-defined function to compare the average weekly percentage change 
-- in COVID deaths for each jurisdiction to the average crude COVID rate for that jurisdiction.

CREATE PROCEDURE Calculate_Average_Percentage_Change
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SELECT Jurisdiction_Residence, 
           AVG(pct_change_wk) AS average_percentage_change,
           @StartDate AS start_date,
           @EndDate AS end_date
    FROM Covid19
    WHERE data_period_start >= @StartDate AND data_period_end <= @EndDate
    GROUP BY Jurisdiction_Residence;
END;


CREATE FUNCTION Calculate_Average_Crude_COVID_Rate
    (@Jurisdiction VARCHAR(100))
RETURNS DECIMAL(10,2)
AS
BEGIN
    DECLARE @AverageCrudeCOVIDRate DECIMAL(10,2);
    SET @AverageCrudeCOVIDRate = (
        SELECT AVG(crude_COVID_rate)
        FROM COVID19
        WHERE Jurisdiction_Residence = @Jurisdiction
    );
    RETURN @AverageCrudeCOVIDRate;
END;

DECLARE @StartDate DATE;
DECLARE @EndDate DATE;
SET @StartDate = '2022-01-01';
SET @EndDate = '2022-12-31';

-- Execute the stored procedure
EXEC Calculate_Average_Percentage_Change @StartDate, @EndDate;

-- Use the function to get the average crude COVID rate for a jurisdiction
DECLARE @Jurisdiction VARCHAR(100);
SET @Jurisdiction = 'Jurisdiction_Residence';
SELECT dbo.Calculate_Average_Crude_COVID_Rate(@Jurisdiction) AS average_crude_covid_rate;
