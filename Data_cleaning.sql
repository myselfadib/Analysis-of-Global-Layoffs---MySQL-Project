-- Data Cleaning 
use world_layoffs ;
Select * from layoffs;

-- Things that gonna be done in this project
-- 1. Remove Duplicates
-- 2. Standardize the data
-- 3. Null Values or blank values
-- 4. Remove Any Columns or Rows

-- creating another table duplicating  raw layoffs table, so that we dont impact the raw data and switch back to raw data if anything unppleasant happens
 create table  layoffs_staging
 like layoffs;
 
 Select * from layoffs_staging;
 
 -- inserting data into it from raw table
 
 insert layoffs_staging 
 select * from layoffs;
 -- removing duplicate
 -- here there is no unique identfier of the table, so removing duplicate is bit tricky
 select *, 
 row_number() over(
 partition by company,location,industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
 from layoffs_staging ;
 
 with duplicate_cte as 
 (  select *, 
 row_number() over(
 partition by company,location,industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
 from layoffs_staging 
 )
 select * from duplicate_cte
 where row_num > 1;
 -- creeating another table to alter row_num
 CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

 
 insert into layoffs_staging2
 select *, 
 row_number() over(
 partition by company,location,industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
 from layoffs_staging ;
 
 select * from layoffs_staging2
 where row_num > 1;
 
 -- finally removing the duplicate
 delete from layoffs_staging2 where row_num>1;
 
select * from layoffs_staging2
 where row_num > 1;
 
 
 -- Standardizing data
 
 select distinct company
 from layoffs_staging2;
 
 -- lets remov e white space before and after company name
 
 update layoffs_staging2
 set company = trim(company);
 
 -- now move to industry column
 select distinct industry
 from layoffs_staging2
 order by 1;
 
 select * from layoffs_staging2
 where industry like 'Crypto%';
 
 -- updating all industry name to crypto who have crypto name in it
 
 update layoffs_staging2
 set industry = 'Crypto'
 where industry like 'Crypto%';
 
 select distinct industry from layoffs_staging2 order by 1;
 
 -- now move to loctaion column
 select distinct location from layoffs_staging2 order by 1;
 
 -- country column
  select distinct country from layoffs_staging2 order by 1;
  
  select country from layoffs_staging2 where
  country like '%.' ;
  
  update layoffs_staging2
  set country = 'United States'
  where country like '%.';
  
  -- lets work with date column which is in text format
  select 'date', str_to_date(`date`, '%m/%d/%Y')
  from datelayoffs_staging2;
  
  update layoffs_staging2
  set `date` = str_to_date(`date`, '%m/%d/%Y');

  select `date` from layoffs_staging2;
  
  -- now changing the structure of database table 
  alter table layoffs_staging2
  modify column `date` date;
  
  -- working with null values
  
  select * from layoffs_staging2
  where total_laid_off is null and percentage_laid_off is null;
  
  select distinct industry from layoffs_staging2;
  
  select * from layoffs_staging2 
  where industry is null or
  industry = '';
  
  select * from layoffs_staging2 
  where company = 'Carvana';
  
  -- we can populate the null/blank industry column of company carvana by taking info from th already populated column, same for other infustrued too
  select * from layoffs_staging2 t1
  join layoffs_staging2 t2 
  on t1.company = t2.company
  and t1.location = t2.location 
  where (t1.industry is null or t1.industry = '')
  and (t2.industry is not null and t2.industry <>'') ;
  
  update layoffs_staging2 t1
  join layoffs_staging2 t2
  on t1.company = t2.company and t1.location= t2.location
  set t1.industry = t2.industry 
    where (t1.industry is null or t1.industry = '')
  and (t2.industry is not null and t2.industry <>'') ;
  
  select company,industry from layoffs_staging2 where industry is null order by 1;
  
  select * from layoffs_staging2 where company = 'Bally''s Interactive';
  
  select * from layoffs_staging2 
  where total_laid_off is null and percentage_laid_off is null;
  
  -- lets delete those rows containig total and percentage laid off null
  
  delete from 
  layoffs_staging2
  where total_laid_off is null and percentage_laid_off is null;
  
  select * from layoffs_staging2 ;
  
  -- dropping row_num column
  alter table layoffs_staging2
  drop column row_num;
  select * from layoffs_staging2;
  
  -- now im done with data cleaning processes. now it's to to move on to do EDA on these data
  