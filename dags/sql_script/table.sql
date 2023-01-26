CREATE TABLE public.d_news (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	author varchar NULL,
	title varchar NULL,
	"source" varchar NULL,
	pubdate varchar NULL,
	category varchar NULL,
	CONSTRAINT d_news_pk PRIMARY KEY (id)
);