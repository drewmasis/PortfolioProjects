--test

SELECT *
FROM `st-project-412720.Portfolio_Project.deaths` 
WHERE continent is not null
ORDER BY 3,4


SELECT *
FROM `st-project-412720.Portfolio_Project.vaccines` 
WHERE continent is not null
ORDER BY 3,4


--data selecting
SELECT location, date, total_cases, new_cases, population
FROM `st-project-412720.Portfolio_Project.deaths`
ORDER BY 1,2


--total cases vs total deaths for US
--likelihood of death
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathPercentage
FROM `st-project-412720.Portfolio_Project.deaths`
WHERE location like '%States%' and continent is not null
ORDER BY 1,2


--total case vs population
--shows % of pop that got covid
SELECT location, date, population, total_cases, (total_cases/population)*100 as PopContracted
FROM `st-project-412720.Portfolio_Project.deaths`
WHERE location like '%States%' and continent is not null
ORDER BY 1,2


--highest infection rates compared to pop
SELECT location, population, MAX(total_cases) as HighestInfectionCount, MAX((total_cases/population))*100 as PercentInfected
FROM `st-project-412720.Portfolio_Project.deaths`
WHERE continent is not null
GROUP BY location, population
ORDER BY PercentInfected desc


--highest total deaths per pop
--casting as int for accuracy
SELECT location, MAX(cast(total_deaths as int)) as total_death_count
FROM `st-project-412720.Portfolio_Project.deaths`
WHERE continent is not null
GROUP BY location
ORDER BY total_death_count desc


--fixed to get rid of continent
--added to every script
SELECT location, MAX(cast(total_deaths as int)) as total_death_count
FROM `st-project-412720.Portfolio_Project.deaths`
WHERE continent is not null
GROUP BY location
ORDER BY total_death_count desc


--by continent
--CORRECT WAY
SELECT location, MAX(cast(total_deaths as int)) as total_death_count
FROM `st-project-412720.Portfolio_Project.deaths`
WHERE continent is null
GROUP BY location
ORDER BY total_death_count desc


--continent w/ highest death count
SELECT continent, MAX(cast(total_deaths as int)) as total_death_count
FROM `st-project-412720.Portfolio_Project.deaths`
WHERE continent is not null
GROUP BY continent
ORDER BY total_death_count desc


--visuals
--global #s
SELECT date, SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(new_cases)*100 as DeathPercentage
FROM `st-project-412720.Portfolio_Project.deaths`
WHERE continent is not null
GROUP BY date
ORDER BY 1,2

--total cases
SELECT SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(new_cases)*100 as DeathPercentage
FROM `st-project-412720.Portfolio_Project.deaths`
WHERE continent is not null
--GROUP BY date
ORDER BY 1,2


--joining tables
SELECT *
FROM `st-project-412720.Portfolio_Project.deaths` as deaths
  JOIN `st-project-412720.Portfolio_Project.vaccines` as vaccines
  ON deaths.location = vaccines.location
  and deaths.date = vaccines.date


  --total pop vs vaccinations
SELECT deaths.continent, deaths.location, deaths.date, deaths.population, vaccines.new_vaccinations
FROM `st-project-412720.Portfolio_Project.deaths` as deaths
  JOIN `st-project-412720.Portfolio_Project.vaccines` as vaccines
  ON deaths.location = vaccines.location
  and deaths.date = vaccines.date
  WHERE deaths.continent is not null
  ORDER BY 2,3


--rolling count
--sum(cast(_, as int)) OR sum(convert(int, _))
SELECT deaths.continent, deaths.location, deaths.date, deaths.population, vaccines.new_vaccinations
,SUM(cast(vaccines.new_vaccinations as int)) OVER (partition by deaths.location ORDER BY deaths.location, deaths.date) as rolling_people_vax
FROM `st-project-412720.Portfolio_Project.deaths` as deaths
  JOIN `st-project-412720.Portfolio_Project.vaccines` as vaccines
  ON deaths.location = vaccines.location
  and deaths.date = vaccines.date
  WHERE deaths.continent is not null
  ORDER BY 2,3


--CTE
--DOES NOT WORK IN BIGQUERY
WITH PopVsVax (continent, location, date, population, new_vaccinations, rolling_people_vax)
as
(
SELECT deaths.continent, deaths.location, deaths.date, deaths.population, vaccines.new_vaccinations
,SUM(cast(vaccines.new_vaccinations as int)) OVER (partition by deaths.location ORDER BY deaths.location, deaths.date) as rolling_people_vax
FROM `st-project-412720.Portfolio_Project.deaths` as deaths
  JOIN `st-project-412720.Portfolio_Project.vaccines` as vaccines
  ON deaths.location = vaccines.location
  and deaths.date = vaccines.date
  WHERE deaths.continent is not null
  --ORDER BY 2,3
)
SELECT *
FROM PopVsVax


--OR
--In BigQuery you need to add a subquery before adding it to the main query

WITH VaccinationSum AS (
  SELECT
    location,
    date,
    SUM(CAST(new_vaccinations AS INT64)) AS rolling_people_vax
  FROM
    `st-project-412720.Portfolio_Project.vaccines`
  GROUP BY
    location, date
)

SELECT
  deaths.continent,
  deaths.location,
  deaths.date,
  deaths.population,
  vaccines.new_vaccinations,
  vs.rolling_people_vax
FROM
  `st-project-412720.Portfolio_Project.deaths` AS deaths
JOIN
  `st-project-412720.Portfolio_Project.vaccines` AS vaccines
ON
  deaths.location = vaccines.location
  AND deaths.date = vaccines.date
JOIN
  VaccinationSum AS vs
ON
  deaths.location = vs.location
  AND deaths.date = vs.date
WHERE
  deaths.continent IS NOT NULL;


--calculation
--NOT CORRECT ON BIQUERY
WITH PopVsVax (continent, location, date, population, new_vaccinations, rolling_people_vax)
as
(
SELECT deaths.continent, deaths.location, deaths.date, deaths.population, vaccines.new_vaccinations
,SUM(cast(vaccines.new_vaccinations as int)) OVER (partition by deaths.location ORDER BY deaths.location, deaths.date) as rolling_people_vax
FROM `st-project-412720.Portfolio_Project.deaths` as deaths
  JOIN `st-project-412720.Portfolio_Project.vaccines` as vaccines
  ON deaths.location = vaccines.location
  and deaths.date = vaccines.date
  WHERE deaths.continent is not null
  --ORDER BY 2,3
)
SELECT *, (rolling_people_vax/population)*100
FROM PopVsVax



--CORRECT CALC %
WITH PopVsVax AS (
  SELECT
    deaths.continent,
    deaths.location,
    deaths.date,
    deaths.population,
    vaccines.new_vaccinations,
    SUM(CAST(vaccines.new_vaccinations AS INT64)) OVER (PARTITION BY deaths.location ORDER BY deaths.date) AS rolling_people_vax
  FROM
    `st-project-412720.Portfolio_Project.deaths` AS deaths
  JOIN
    `st-project-412720.Portfolio_Project.vaccines` AS vaccines
  ON
    deaths.location = vaccines.location
    AND deaths.date = vaccines.date
  WHERE
    deaths.continent IS NOT NULL
)
SELECT
  *,
  (rolling_people_vax / population) * 100 AS vaccination_percentage
FROM
  PopVsVax;



--DOES NOT WORK ON BIG QUERY
--TEMP TABLES
DROP TABLE if exists #PercentPopVax
CREATE TABLE #PercentPopVax
(
  continent nvarchar(255),
  location nvvarchar(255),
  date datetime,
  population numeric,
  new_vaccinations numeric,
  rolling_people_vax numeric
)
INSERT INTO #PercentPopVax
SELECT deaths.continent, deaths.location, deaths.date, deaths.population, vaccines.new_vaccinations
,SUM(cast(vaccines.new_vaccinations as int)) OVER (partition by deaths.location ORDER BY deaths.location, deaths.date) as rolling_people_vax
FROM `st-project-412720.Portfolio_Project.deaths` as deaths
  JOIN `st-project-412720.Portfolio_Project.vaccines` as vaccines
  ON deaths.location = vaccines.location
  and deaths.date = vaccines.date
  WHERE deaths.continent is not null
  --ORDER BY 2,3
SELECT
  *,
  (rolling_people_vax / population) * 100 AS vaccination_percentage
FROM
  #PercentPopVax;


--USE THIS FOR BIGQUERY TEMP TABLES
-- Create a temporary table
-- Start of SQL script
DECLARE rolling_people_vax INT64;

CREATE TEMP TABLE PercentPopVax AS
SELECT
  deaths.continent,
  deaths.location,
  deaths.date,
  deaths.population,
  vaccines.new_vaccinations,
  SUM(CAST(vaccines.new_vaccinations AS INT64)) OVER (PARTITION BY deaths.location ORDER BY deaths.date) AS rolling_people_vax
FROM
  `st-project-412720.Portfolio_Project.deaths` AS deaths
JOIN
  `st-project-412720.Portfolio_Project.vaccines` AS vaccines
ON
  deaths.location = vaccines.location
  AND deaths.date = vaccines.date
WHERE
  deaths.continent IS NOT NULL;

-- Query the temporary table with additional calculations
SELECT
  *,
  (rolling_people_vax / population) * 100 AS vaccination_percentage
FROM
  PercentPopVax;
-- End of SQL script






--create view for visuals
CREATE VIEW `st-project-412720._script989be1a4d64918c81cf7246beb9021501a02db53.PercentPopVax` AS
SELECT
  deaths.continent,
  deaths.location,
  deaths.date,
  deaths.population,
  vaccines.new_vaccinations,
  SUM(CAST(vaccines.new_vaccinations AS INT64)) OVER (PARTITION BY deaths.location ORDER BY deaths.date) AS rolling_people_vax,
  (SUM(CAST(vaccines.new_vaccinations AS INT64)) OVER (PARTITION BY deaths.location ORDER BY deaths.date) / deaths.population) * 100 AS vaccination_percentage
FROM
  `st-project-412720.Portfolio_Project.deaths` AS deaths
JOIN
  `st-project-412720.Portfolio_Project.vaccines` AS vaccines
ON
  deaths.location = vaccines.location
  AND deaths.date = vaccines.date
WHERE
  deaths.continent IS NOT NULL;




--OVERALL VIEW
SELECT *
FROM `st-project-412720._script989be1a4d64918c81cf7246beb9021501a02db53.PercentPopVax`;
