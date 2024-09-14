-- Data cleaning

select *
from layoffs;

-- Ahora, cuando limpiamos datos, generalmente seguimos algunos pasos:
-- 1. Verificar si hay duplicados y eliminar los que haya 
-- 2. Estandarizar los datos y corregir los errores 
-- 3. Mirar los valores nulos y cuales pueden ser eliminados o reemplazados
-- 4. Eliminar los columnas y filas que no son necesarias


#Vamos primeramente a crear una copia de la tabla, en la cual haremos los cambios

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;


#Entonces para eliminar los duplicados, primeramente vamos a crear una columna para identificar

select *,
row_number () over (
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country,
funds_raised_millions) as row_num
from layoffs_staging;

#Ahora vamos a filtrar las filas que tienen el row_num > 1, para ello creamos un cte

with duplicate_cte as
(
select *,
row_number () over (
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;

#Ahora creamos otra tabla en donde vamos a eliminar los duplicados
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
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

#Proporcionamos los datos
insert into layoffs_staging2
select *,
row_number () over (
partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;

#Finalmente eliminamos estos datos
delete
from layoffs_staging2 where row_num>1;

#Comprobamos 
select *
from layoffs_staging2;


-- Estandarizacion de datos
#Podemos ver que hay columnas que tienen un espacio al principio

select company, trim(company)
from layoffs_staging2;

#Vamos a dejar los datos sin este espacio inicial, para ello;
update layoffs_staging2
set company=trim(company);

#Veamos el estado de las otras columnas
select distinct industry
from layoffs_staging2
order by 1;

#Podemos ver que existen espacios en blanco y nulos, asi como categorias repetidas como la de crypto, entonce vamos agrupar estas categorias.

select *
from layoffs_staging2
where industry like 'crypto%';	

update layoffs_staging2
set industry= 'Crypto'
where industry like 'crypto%';	

#Ahora veamos la columna location y country
select distinct location
from layoffs_staging2
order by 1;

select distinct country
from layoffs_staging2
order by 1;

#Podemos observar que en la columna country tenemos United States en dos formatos, vamos a juntar en uno mismo
#Debido a que uno de los formatos tiene un punto final, vamos a usar trailing para quitar el punto

update layoffs_staging2
set country = TRIM(trailing '.' from country)
where country like 'United States%';

#Comprobamos el resultado
select distinct country
from layoffs_staging2
order by 1;

#Ahora debido a que tenemos la columna date en formato de texto, vamos a cambiar al formato de fecha o date;

select `date`,
str_to_date(`date`, '%m/%d/%y')
from layoffs_staging2;

#Entonces actualizamos la columna date;
update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

#Cambiamos tambien la definicion de la columna
alter table layoffs_staging2
modify column `date` date;

#Ahora, vamos a quitar los valores en blanco y los valores nulos

select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null
;

#Podemos observar para este caso que tenemos valores nulos y blancos en industry para la misma compania.
#Entonces nuestro objetivo es copiar los valores existentes en los espacios nulos o blancos.

select t1.company, t1.industry as industry_null, t2.industry as industry_not_null
from layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company=t2.company
where (t1.industry is null or t1.industry='')
and t2.industry is not null;

#Para facilitar, vamos a dejar todo en un mismo formato. Entonces vamos primeramente dejar los espacios en blanco como nulos

select * 
from layoffs_staging2
where industry is null
or industry=''; 

update layoffs_staging2
set industry=null
where industry='';

#Ahora finalmente actualizamos la tabla
update layoffs_staging2 as t1
join layoffs_staging2 as t2
	on t1.company=t2.company
set t1.industry=t2.industry
where t1.industry is null and t2.industry is not null;

#Ahora como ya no vamos a necesitar la columna row_num antes utilizada, podemos eliminarla;

alter table layoffs_staging2
drop column row_num;

#Con esto finalmente tenemos la tabla final limpia
select *
from layoffs_staging2;

#Seguimos teniendo valores nulos en las columnas total_laid_off y percentage_laid_off, pero como no tenemos informacion para completar, la dejamos asi.
#Pues no seria muy inteligente eliminar las filas que contienen estos valores nulos





