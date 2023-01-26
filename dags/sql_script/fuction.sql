CREATE OR REPLACE PROCEDURE public.end_table(IN p_path text)
 LANGUAGE plpgsql
AS $procedure$
begin
with category_day_count as (
		select dn.category, 
				date(to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS')) as d,
				count(*) as day_count
		from d_news dn 
		group by dn.category, date(to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS'))
		), category_day_count_group as (
		select t.category, avg(t.day_count) as day_count_avg, max(t.day_count) as day_count_max
		from category_day_count as t
		group by t.category
		), category_day_count_group_t as (
		select distinct t.category, t.day_count_avg, c.d as day_count_max, t.day_count_max as df
		from category_day_count_group as t
			inner join category_day_count as c on c.category = t.category and t.day_count_max = c.day_count
		)
	select dn.category, 
		count(*) as all_count,
		sum(case when dn."source" = 'https://www.vedomosti.ru/rss/news' then 1 else 0 end) as vedomosti_count,
		sum(case when dn."source" = 'https://lenta.ru/rss/' then 1 else 0 end) as lenta_count,
		sum(case when dn."source" = 'https://tass.ru/rss/v2.xml' then 1 else 0 end) as tass_count,
		sum(case when to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS') > now() - interval '1 day' then 1 else 0 end) as day_all_count,
		sum(case when to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS') > now() - interval '1 day' and dn."source" = 'https://www.vedomosti.ru/rss/news' then 1 else 0 end) as day_vedomosti_count,
		sum(case when to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS') > now() - interval '1 day' and dn."source" = 'https://lenta.ru/rss/' then 1 else 0 end) as day_lenta_count,
		sum(case when to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS') > now() - interval '1 day' and dn."source" = 'https://tass.ru/rss/v2.xml' then 1 else 0 end) as day_tass_count,
		t.day_count_avg,
		t.day_count_max,	
		sum(case when date_part('isodow', to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS')) = 1 then 1 else 0 end) as day1_count,
		sum(case when date_part('isodow', to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS')) = 2 then 1 else 0 end) as day2_count,
		sum(case when date_part('isodow', to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS')) = 3 then 1 else 0 end) as day3_count,
		sum(case when date_part('isodow', to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS')) = 4 then 1 else 0 end) as day4_count,
		sum(case when date_part('isodow', to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS')) = 5 then 1 else 0 end) as day5_count,
		sum(case when date_part('isodow', to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS')) = 6 then 1 else 0 end) as day6_count,
		sum(case when date_part('isodow', to_timestamp(dn.pubdate,'Dy, dd Mon YYYY HH24:MI:SS')) = 7 then 1 else 0 end) as day7_count
	from d_news dn 
		inner join category_day_count_group_t t on dn.category = t.category
	group by dn.category,
		t.day_count_avg,
		t.day_count_max;
end;$procedure$
;