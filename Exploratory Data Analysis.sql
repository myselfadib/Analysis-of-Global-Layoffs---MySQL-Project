-- Exploratory Data Analysis 

Select * from layoffs_staging2;
-- maximum number rof total lay off 
select max(total_laid_off) from layoffs_staging2;

select max(percentage_laid_off) from layoffs_staging2;

select * from layoffs_staging2 where percentage_laid_off=1
order by total_laid_off desc;

select * from layoffs_staging2 where percentage_laid_off=1
order by funds_raised_millions desc;

-- per company's total laid off number 
select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- lets findout what periods of data we ahve
select max(`date`) , min(`date`) from layoffs_staging2;

-- per industry's total laid off number 
select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

-- per country's total laid off number 
select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

-- per day's total laid off number 
select `date`, sum(total_laid_off)
from layoffs_staging2
group by `date`
order by 2 desc;

-- to extend it to see per year
select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 2 desc;

-- per stage of the company
select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

-- lets see avg percentage layoffs of companys
select company, avg(percentage_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

-- progression of lay off (rolling sum)
select * from layoffs_staging2;

select substring(`date`,6,2) as `Month`, sum(total_laid_off)
from layoffs_staging2
group by `month`;

select substring(`date`,1,7) as `month`, sum(total_laid_off) from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 2 asc ;

select  * from layoffs_staging2 where total_laid_off is not null order by total_laid_off asc;
-- applying rolling sum on this

with Rolling_Total as (
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
 from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total_off,sum(total_off) over(order by `month`) as rolling_total
from Rolling_Total;

-- rolling total lay off by company and year

select company, sum(total_laid_off) as total_off
from layoffs_staging2
group by company
order by 1
;

with company_layoff as 
( select company,year(`date`) as `year`, sum(total_laid_off) as total_off
from layoffs_staging2
group by company,year(`date`)
order by 1
)
select company,`year`, total_off, sum(total_off) over(order by company)
from company_layoff;

-- rank the companies based of numbe rof layoffs

select company, year(`date`) as `year`, sum(total_laid_off) 
from layoffs_staging2
group by company,year(`date`)
order by 3 desc;

with cte(company, years, total_laid_off) as
(
select company, year(`date`) as `year`, sum(total_laid_off) 
from layoffs_staging2
group by company,year(`date`)
)
select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
from cte
where years is not null
order by ranking asc;

-- now lets rank them to filter out top N(5/10)
with cte(company, years, total_laid_off) as
(
select company, year(`date`) as `year`, sum(total_laid_off) 
from layoffs_staging2
group by company,year(`date`)
), company_rank as (
select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
from cte
where years is not null
order by ranking asc)
select * from company_rank
where ranking <=5;

