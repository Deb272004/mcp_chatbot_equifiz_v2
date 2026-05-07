--
-- PostgreSQL database dump
--

\restrict 0f1UvOsnZEokuRl7BJeIU6dyjSqbeniB2wOCCKTwuyW8gneSuBswHzHGUcDv9Ef

-- Dumped from database version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: companies; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.companies (
    co_code integer NOT NULL,
    bsecode character varying(50),
    nsesymbol character varying(50),
    companyname character varying(255),
    companyshortname character varying(100),
    categoryname character varying(100),
    isin character varying(50),
    bsegroup character varying(50),
    mcaptype character varying(50),
    sectorcode character varying(50),
    sectorname character varying(100),
    industrycode character varying(50),
    industryname character varying(100),
    bselistedflag character varying(10),
    nselistedflag character varying(10),
    displaytype character varying(50),
    synced_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.companies OWNER TO postgres;

--
-- Name: company_master; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.company_master (
    co_code integer NOT NULL,
    bsecode character varying(20),
    nsesymbol character varying(20),
    companyname character varying(200),
    companyshortname character varying(100),
    categoryname character varying(100),
    isin character varying(20),
    bsegroup character varying(10),
    mcaptype character varying(50),
    sectorcode character varying(50),
    sectorname character varying(100),
    industrycode character varying(50),
    industryname character varying(100),
    bselistedflag character varying(5),
    nselistedflag character varying(5),
    displaytype character varying(20),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.company_master OWNER TO postgres;

--
-- Name: sync_meta; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.sync_meta (
    key text NOT NULL,
    value text NOT NULL
);


ALTER TABLE public.sync_meta OWNER TO postgres;

--
-- Data for Name: companies; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.companies (co_code, bsecode, nsesymbol, companyname, companyshortname, categoryname, isin, bsegroup, mcaptype, sectorcode, sectorname, industrycode, industryname, bselistedflag, nselistedflag, displaytype, synced_at) FROM stdin;
270			India Cements Ltd	India Cements	Company	INE383A01012		Small Cap	00000011	Cement	00000019	Cement - South India			MAN	2026-04-15 21:08:28.168384
106			Century Enka Ltd	Century Enka	Company	INE485A01015		Small Cap	00000062	Textiles	00000095	Textiles - Manmade			MAN	2026-04-15 21:11:21.52842
29			Arvind Ltd	Arvind Ltd	Company	INE034A01011		Small Cap	00000062	Textiles	00000094	Textiles - Cotton/Blended			MAN	2026-04-15 21:07:44.980549
19			Andhra Cements Ltd	Andhra Cements	Company	INE666E01020		Small Cap	00000011	Cement	00000019	Cement - South India			MAN	2026-04-15 21:11:25.111663
726			Andrew Yule & Company Ltd	Andrew Yule & Co	Company	INE449C01025		Small Cap	00000021	Diversified	00000109	Diversified - Medium / Small			MAN	2026-04-15 21:08:33.406695
43			Atul Ltd	Atul	Company	INE100A01010		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:11:25.111663
93			Britannia Industries Ltd	Britannia Inds.	Company	INE216A01030		Large Cap	00000027	FMCG	00000054	Food - Processing - MNC			MAN	2026-04-15 21:11:25.111663
100			Oriental Aromatics Ltd	Oriental Aromat.	Company	INE959C01023		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:11:25.111663
115			Citurgia Biochemicals Ltd	Citurgia Biochem	Company	INE795B01031		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:11:25.111663
146			Fermenta Biotech Ltd	Fermenta Biotec.	Company	INE225B01021		Small Cap	00000046	Pharmaceuticals	00000071	Pharmaceuticals - Indian - Bulk Drugs			MAN	2026-04-15 21:11:25.111663
225			Gujarat Petrosynthese Ltd	Guj. Petrosynth.	Company	INE636P01011		Small Cap	00000048	Plastic products	00000075	Plastics Products			MAN	2026-04-15 21:11:25.111663
298			Tata Investment Corporation Ltd	Tata Inv.Corpn.	Company	INE672A01026		Mid Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:11:25.111663
38			Asi Industries Ltd	Asi Industries	Company	INE443A01030		Small Cap	00000038	Mining & Mineral products	00000059	Mining / Minerals / Metals			MAN	2026-04-15 21:07:44.980549
156			Elpro International Ltd	Elpro Internatio	Company	INE579B01039		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:07:44.980549
295			Ingersoll-Rand (India) Ltd	Ingersoll-Rand	Company	INE177A01018		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000025	Compressors / Drilling Equipment			MAN	2026-04-15 21:07:44.980549
305			Jaykay Enterprises Ltd	Jaykay Enter.	Company	INE903A01025		Small Cap	00000085	Aerospace & Defence	00000044	Engineering			MAN	2026-04-15 21:07:44.980549
396			HeidelbergCement India Ltd	Heidelberg Cem.	Company	INE578A01017		Small Cap	00000011	Cement	00000018	Cement - North India			MAN	2026-04-15 21:07:44.980549
513			SRF Ltd	SRF	Company	INE647A01010		Mid Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:07:44.980549
681			Majestic Auto Ltd	Majestic Auto	Company	INE201B01022		Small Cap	00000021	Diversified	00000109	Diversified - Medium / Small			MAN	2026-04-15 21:07:44.980549
120			Computer Point Ltd	Computer Point	Company	INE607B01012		Small Cap	00000064	Trading	00000101	Trading			MAN	2026-04-15 21:08:28.168384
117			DIC India Ltd	DIC India	Company	INE303A01010		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:11:21.52842
348			Larsen & Toubro Ltd	Larsen & Toubro	Company	INE018A01030		Large Cap	00000032	Infrastructure Developers & Operators	00000045	Engineering - Turnkey Services			MAN	2026-04-15 21:11:21.52842
392			Mukand Ltd	Mukand	Company	INE304A01026		Small Cap	00000057	Steel	00000085	Steel - Medium / Small			MAN	2026-04-15 21:11:21.52842
639			Automobile Products of India Ltd	Auto.Prod.India	Company	INE0NY101012		Small Cap	00000039	Miscellaneous	00000106	Miscellaneous			MAN	2026-04-15 21:11:21.52842
655			Global Offshore Services Ltd	Global Offshore	Company	INE446C01013		Small Cap	00000056	Shipping	00000082	Shipping			MAN	2026-04-15 21:11:21.52842
1054			JTEKT India Ltd	JTEKT India	Company	INE643A01035		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:11:21.52842
1071			Kirloskar Industries Ltd	Kirloskar Indus.	Company	INE250A01039		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:11:21.52842
1181			Sika Interplant Systems Ltd	Sika Interplant	Company	INE438E01032		Small Cap	00000085	Aerospace & Defence	00000044	Engineering			MAN	2026-04-15 21:11:21.52842
135			Deepak Fertilisers & Petrochemicals Corp Ltd	Deepak Fertilis.	Company	INE501A01019		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:08:28.168384
141			DCW Ltd	DCW	Company	INE500A01029		Small Cap	00000045	Petrochemicals	00000069	Petrochemicals			MAN	2026-04-15 21:08:28.168384
184			Gabriel India Ltd	Gabriel India	Company	INE524A01029		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:28.168384
185			Gajra Bevel Gears Ltd	Gajra Bevel	Company	INE282D01010		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:28.168384
382			Mipco Seamless Rings (Gujarat) Ltd	Mipco Seaml Ring	Company	INE860N01012		Small Cap	00000034	IT - Software	00000028	Computers - Software - Medium / Small			MAN	2026-04-15 21:08:28.168384
662			HB Leasing & Finance Co Ltd	HB Leasing &Fin.	Company	INE549B01016		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:08:28.168384
720			V I P Industries Ltd	V I P Inds.	Company	INE054A01027		Small Cap	00000048	Plastic products	00000060	Moulded Luggage			MAN	2026-04-15 21:08:28.168384
303			J B Chemicals & Pharmaceuticals Ltd	J B Chemicals &	Company	INE572A01036		Small Cap	00000046	Pharmaceuticals	00000070	Pharmaceuticals - Indian - Bulk Drugs & Formln			MAN	2026-04-15 21:11:25.111663
737			Dhunseri Ventures Ltd	Dhunseri Vent.	Company	INE477B01010		Small Cap	00000064	Trading	00000101	Trading			MAN	2026-04-15 21:08:28.168384
17			Amrutanjan Health Care Ltd	Amrutanjan Healt	Company	INE098F01031		Small Cap	00000046	Pharmaceuticals	00000072	Pharmaceuticals - Indian - Formulations			MAN	2026-04-15 21:11:23.178081
70			Bharat Gears Ltd	Bharat Gears	Company	INE561C01019		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:11:23.178081
176			Foods & Inns Ltd	Foods & Inns	Company	INE976E01023		Small Cap	00000027	FMCG	00000053	Food - Processing - Indian			MAN	2026-04-15 21:08:54.717002
288			Gillette India Ltd	Gillette India	Company	INE322A01010		Small Cap	00000027	FMCG	00000066	Personal Care - Multinational			MAN	2026-04-15 21:08:54.717002
322			Kalyani Steels Ltd	Kalyani Steels	Company	INE907A01026		Small Cap	00000057	Steel	00000085	Steel - Medium / Small			MAN	2026-04-15 21:08:54.717002
345			Trent Ltd	Trent	Company	INE849A01020		Large Cap	00000054	Retail	00000101	Trading			MAN	2026-04-15 21:08:54.717002
435			PCBL Chemical Ltd	PCBL Chemical	Company	INE602A01031		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:08:54.717002
23			Apollo Tyres Ltd	Apollo Tyres	Company	INE438A01022		Small Cap	00000065	Tyres	00000105	Tyres			MAN	2026-04-15 21:08:59.842413
155			Elgi Equipments Ltd	Elgi Equipments	Company	INE285A01027		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000025	Compressors / Drilling Equipment			MAN	2026-04-15 21:11:23.178081
745			Williamson Magor & Company Ltd	Williamson Magor	Company	INE210A01017		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:07:44.980549
872			Bharat Agri Fert & Realty Ltd	Bharat Agri Fert	Company	INE842D01029		Small Cap	00000031	Hotels & Restaurants	00000057	Hotels			MAN	2026-04-15 21:07:44.980549
24			Utique Enterprises Ltd	Utique Enterp.	Company	INE096A01010		Small Cap	00000064	Trading	00000101	Trading			MAN	2026-04-15 21:09:05.115581
37			SKF India Ltd	SKF India	Company	INE640A01023		Small Cap	00000066	Bearings	00000013	Bearings			MAN	2026-04-15 21:09:05.115581
42			Atlas Cycles (Haryana) Ltd	Atlas Cycles	Company	INE446A01025		Small Cap	00000017	Consumer Durables	00000033	Cycles And Accessories			MAN	2026-04-15 21:08:59.842413
181			Graviss Hospitality Ltd	Graviss Hospital	Company	INE214F01026		Small Cap	00000031	Hotels & Restaurants	00000057	Hotels			MAN	2026-04-15 21:11:23.178081
50			Bajaj Holdings & Investment Ltd	Bajaj Holdings	Company	INE118A01012		Large Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:08:59.842413
102			Graphite India Ltd	Graphite India	Company	INE371A01025		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000040	Electrodes - Graphites			MAN	2026-04-15 21:09:05.115581
249			Bajaj Hindusthan Sugar Ltd	Bajaj Hindusthan	Company	INE306A01021		Small Cap	00000059	Sugar	00000088	Sugar			MAN	2026-04-15 21:09:05.115581
190			Garware Technical Fibres Ltd	Garware Tech.	Company	INE276A01018		Small Cap	00000062	Textiles	00000097	Textiles - Products			MAN	2026-04-15 21:08:59.842413
208			Greaves Cotton Ltd	Greaves Cotton	Company	INE224A01026		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000046	Engines			MAN	2026-04-15 21:08:59.842413
372			Max Financial Services Ltd	Max Financial	Company	INE180A01020		Mid Cap	00000039	Miscellaneous	00000106	Miscellaneous			MAN	2026-04-15 21:08:59.842413
378			Bosch Ltd	Bosch	Company	INE323A01026		Large Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:59.842413
211			Him Teknoforge Ltd	Him Teknoforg.	Company	INE705G01021		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:11:23.178081
266			JSW Dulux Ltd	JSW Dulux	Company	INE133A01011		Small Cap	00000043	Paints/Varnish	00000063	Paints / Varnishes			MAN	2026-04-15 21:11:23.178081
702			Rico Auto Industries Ltd	Rico Auto Inds	Company	INE209B01025		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:54.717002
755			AG Ventures Ltd	AG Ventures	Company	INE321D01016		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:08:54.717002
284			Linde India Ltd	Linde India	Company	INE473A01011		Mid Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:09:05.115581
365			Mahindra & Mahindra Ltd	M & M	Company	INE101A01026		Large Cap	00000005	Automobile	00000006	Automobiles - Passenger Cars			MAN	2026-04-15 21:09:05.115581
390			Peninsula Land Ltd	Peninsula Land	Company	INE138A01028		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:09:05.115581
306			JCT Ltd	JCT	Company	INE945A01026		Small Cap	00000062	Textiles	00000093	Textiles - Composite			MAN	2026-04-15 21:11:23.178081
334			Kinetic Engineering Ltd	Kinetic Engg.	Company	INE266B01017		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:11:23.178081
552			Tata Chemicals Ltd	Tata Chemicals	Company	INE092A01019		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:11:23.178081
394			Saptak Chem & Business Ltd	Saptak Chem &	Company	INE467X01023		Small Cap	00000054	Retail	00000101	Trading			MAN	2026-04-15 21:08:59.842413
421			Oriental Hotels Ltd	Oriental Hotels	Company	INE750A01020		Small Cap	00000031	Hotels & Restaurants	00000057	Hotels			MAN	2026-04-15 21:09:05.115581
447			Premier Ltd	Premier	Company	INE342A01018		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000044	Engineering			MAN	2026-04-15 21:09:05.115581
499			Saurashtra Cement Ltd	Saurashtra Cem.	Company	INE626A01014		Small Cap	00000011	Cement	00000018	Cement - North India			MAN	2026-04-15 21:08:59.842413
566			Tata Steel Ltd	Tata Steel	Company	INE081A01020		Large Cap	00000057	Steel	00000084	Steel - Large			MAN	2026-04-15 21:11:23.178081
527			Standard Batteries Ltd	Standard Battery	Company	INE502C01039		Small Cap	00000064	Trading	00000101	Trading			MAN	2026-04-15 21:09:05.115581
175			Nestle India Ltd	Nestle India	Company	INE239A01024		Large Cap	00000027	FMCG	00000054	Food - Processing - MNC			MAN	2026-04-15 21:08:27.42814
965			Jindal Saw Ltd	Jindal Saw	Company	INE324A01032		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000084	Steel - Large			MAN	2026-04-15 21:08:54.717002
44			Automobile Corporation Of Goa Ltd	Auto.Corp.of Goa	Company	INE451C01013		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:52.876314
8			Advance Petrochemicals Ltd	Advance Petroch.	Company	INE334N01018		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:09:01.477119
140			DMCC Speciality Chemicals Ltd	DMCC Speciality	Company	INE505A01010		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:09:01.477119
1200			Carysil Ltd	Carysil	Company	INE482D01024		Small Cap	00000017	Consumer Durables	00000036	Domestic Appliances			MAN	2026-04-15 21:11:21.52842
292			Castrol India Ltd	Castrol India	Company	INE172A01027		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:08:52.876314
296			International Combustion (India) Ltd	Intl. Combustion	Company	INE403C01014		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000044	Engineering			MAN	2026-04-15 21:08:52.876314
409			Naperol Investments Ltd	Naperol Invest.	Company	INE585A01020		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:08:27.42814
195			Glaxosmithkline Pharmaceuticals Ltd	Glaxosmi. Pharma	Company	INE159A01016		Mid Cap	00000046	Pharmaceuticals	00000073	Pharmaceuticals - Multinational			MAN	2026-04-15 21:09:01.477119
429			Panyam Cements & Mineral Industries Ltd	Panyam Cement	Company	INE167E01037		Small Cap	00000011	Cement	00000019	Cement - South India			MAN	2026-04-15 21:08:27.42814
28			Aruna Hotels Ltd	Aruna Hotels	Company	INE957C01019		Small Cap	00000031	Hotels & Restaurants	00000057	Hotels			MAN	2026-04-15 21:11:24.1891
34			Asian Paints Ltd	Asian Paints	Company	INE021A01026		Large Cap	00000043	Paints/Varnish	00000063	Paints / Varnishes			MAN	2026-04-15 21:08:27.42814
90			Abbott India Ltd	Abbott India	Company	INE358A01014		Mid Cap	00000046	Pharmaceuticals	00000073	Pharmaceuticals - Multinational			MAN	2026-04-15 21:08:27.42814
263			Futura Polyesters Ltd	Futura Polyester	Company	INE564A01017		Small Cap	00000062	Textiles	00000095	Textiles - Manmade			MAN	2026-04-15 21:09:01.477119
361			Mafatlal Industries Ltd	Mafatlal Inds.	Company	INE270B01035		Small Cap	00000021	Diversified	00000108	Diversified - Large			MAN	2026-04-15 21:08:52.876314
830			Rajasthan Petro Synthetics Ltd	Rajas. Petro Syn	Company	INE374C01017		Small Cap	00000039	Miscellaneous	00000106	Miscellaneous			MAN	2026-04-15 21:08:54.717002
54			Sanathnagar Enterprises Ltd	Sanathnagar Ent.	Company	INE367E01033		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:11:24.1891
124			Cosmo First Ltd	Cosmo First	Company	INE757A01017		Small Cap	00000042	Packaging	00000062	Packaging			MAN	2026-04-15 21:08:27.42814
299			Ion Exchange (India) Ltd	ION Exchange	Company	INE570A01022		Small Cap	00000032	Infrastructure Developers & Operators	00000045	Engineering - Turnkey Services			MAN	2026-04-15 21:09:01.477119
104			CEAT Ltd	CEAT	Company	INE482A01020		Small Cap	00000065	Tyres	00000105	Tyres			MAN	2026-04-15 21:11:24.1891
134			Deccan Cements Ltd	Deccan Cements	Company	INE583C01021		Small Cap	00000011	Cement	00000019	Cement - South India			MAN	2026-04-15 21:11:24.1891
434			Pfizer Ltd	Pfizer	Company	INE182A01018		Small Cap	00000046	Pharmaceuticals	00000073	Pharmaceuticals - Multinational			MAN	2026-04-15 21:08:27.42814
842			Himatsingka Seide Ltd	Himatsing. Seide	Company	INE049A01027		Small Cap	00000062	Textiles	00000097	Textiles - Products			MAN	2026-04-15 21:08:54.717002
332			Indokem Ltd	Indokem	Company	INE716F01012		Small Cap	00000014	Chemicals	00000038	Dyes And Pigments			MAN	2026-04-15 21:09:01.477119
210			Golden Tobacco Ltd	Golden Tobacco	Company	INE973A01010		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:11:24.1891
436			Team24 Consumer Products Ltd	Team24 Consumer	Company	INE601A01017		Small Cap	00000027	FMCG	00000053	Food - Processing - Indian			MAN	2026-04-15 21:08:27.42814
257			HLV Ltd	HLV	Company	INE102A01024		Small Cap	00000031	Hotels & Restaurants	00000057	Hotels			MAN	2026-04-15 21:11:24.1891
548			Tanfac Industries Ltd	Tanfac Inds.	Company	INE639B01023		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:08:27.42814
344			Panasonic Energy India Company Ltd	Panasonic Energy	Company	INE795A01017		Small Cap	00000022	Dry cells	00000037	Dry Cells			MAN	2026-04-15 21:09:01.477119
444			Prakash Industries Ltd	Prakash Industri	Company	INE603A01013		Small Cap	00000057	Steel	00000085	Steel - Medium / Small			MAN	2026-04-15 21:09:01.477119
500			Scindia Steam Navigation Co. Ltd	Scindia Steam	Company				00000056	Shipping	00000082	Shipping			MAN	2026-04-15 21:09:01.477119
562			Texmaco Infrastructure & Holdings Ltd	Texmaco Infrast.	Company	INE435C01024		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:08:27.42814
218			Ambuja Cements Ltd	Ambuja Cements	Company	INE079A01024		Large Cap	00000011	Cement	00000018	Cement - North India			MAN	2026-04-15 21:11:24.1891
329			Kesoram Industries Ltd	Kesoram Inds.	Company	INE087A01019		Small Cap	00000039	Miscellaneous	00000106	Miscellaneous			MAN	2026-04-15 21:11:24.1891
341			Kothari Sugars & Chemicals Ltd	Kothari Sugars	Company	INE419A01022		Small Cap	00000059	Sugar	00000088	Sugar			MAN	2026-04-15 21:11:24.1891
483			Citadel Realty & Developers Ltd	Citadel Realty	Company	INE906D01014		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:11:24.1891
161			Escorts Kubota Ltd	Escorts Kubota	Company	INE042A01014		Mid Cap	00000005	Automobile	00000007	Automobiles - Tractors			MAN	2026-04-15 21:11:22.295015
245			Hindustan Construction Company Ltd	Hind.Construct.	Company	INE549A01026		Small Cap	00000032	Infrastructure Developers & Operators	00000031	Construction			MAN	2026-04-15 21:11:22.295015
247			Hindustan Motors Ltd	Hindustan Motors	Company	INE253A01025		Small Cap	00000005	Automobile	00000006	Automobiles - Passenger Cars			MAN	2026-04-15 21:11:22.295015
281			IFB Industries Ltd	IFB Industries	Company	INE559A01017		Small Cap	00000017	Consumer Durables	00000036	Domestic Appliances			MAN	2026-04-15 21:11:22.295015
423			Orissa Sponge Iron & Steel Ltd	Orissa Sponge	Company	INE228D01013		Small Cap	00000057	Steel	00000086	Steel - Sponge Iron			MAN	2026-04-15 21:11:22.295015
491			Sadhana Nitro Chem Ltd	Sadhana Nitro	Company	INE888C01040		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:11:22.295015
327			Kaycee Industries Ltd	Kaycee Inds.	Company	INE813G01023		Small Cap	00000008	Capital Goods - Electrical Equipment	00000039	Electric Equipment			MAN	2026-04-15 21:08:56.561419
357			The Ramco Cements Ltd	The Ramco Cement	Company	INE331A01037		Small Cap	00000011	Cement	00000019	Cement - South India			MAN	2026-04-15 21:08:56.561419
476			Reliance Industries Ltd	Reliance Industries	Company	INE002A01018		Large Cap	00000052	Refineries	00000080	Refineries			MAN	2026-04-15 21:08:52.876314
39			Astrazeneca Pharma India Ltd	Astrazeneca Phar	Company	INE203A01020		Small Cap	00000046	Pharmaceuticals	00000073	Pharmaceuticals - Multinational			MAN	2026-04-15 21:08:56.561419
68			Bharat Bijlee Ltd	Bharat Bijlee	Company	INE464A01036		Small Cap	00000008	Capital Goods - Electrical Equipment	00000039	Electric Equipment			MAN	2026-04-15 21:08:24.840641
150			EIH Ltd	EIH	Company	INE230A01023		Small Cap	00000031	Hotels & Restaurants	00000057	Hotels			MAN	2026-04-15 21:08:24.840641
31			Ashok Leyland Ltd	Ashok Leyland	Company	INE208A01029		Mid Cap	00000005	Automobile	00000005	Automobiles - LCVs / HCVs			MAN	2026-04-15 21:08:17.546202
171			Finolex Cables Ltd	Finolex Cables	Company	INE235A01022		Small Cap	00000007	Cables	00000015	Cables - Power			MAN	2026-04-15 21:08:24.840641
304			JK Tyre & Industries Ltd	JK Tyre & Indust	Company	INE573A01042		Small Cap	00000065	Tyres	00000105	Tyres			MAN	2026-04-15 21:08:24.840641
387			Modi Rubber Ltd	Modi Rubber	Company	INE832A01018		Small Cap	00000039	Miscellaneous	00000106	Miscellaneous			MAN	2026-04-15 21:08:24.840641
451			Hawkins Cookers Ltd	Hawkins Cookers	Company	INE979B01015		Small Cap	00000017	Consumer Durables	00000036	Domestic Appliances			MAN	2026-04-15 21:08:24.840641
79			Bliss GVS Pharma Ltd	Bliss GVS Pharma	Company	INE416D01022		Small Cap	00000046	Pharmaceuticals	00000072	Pharmaceuticals - Indian - Formulations			MAN	2026-04-15 21:08:17.546202
154			Electrosteel Castings Ltd	Electrost.Cast.	Company	INE086A01029		Small Cap	00000010	Castings, Forgings & Fastners	00000017	Castings & Forgings			MAN	2026-04-15 21:08:17.546202
183			Delta Manufacturing Ltd	Delta Manufact.	Company	INE393A01011		Small Cap	00000062	Textiles	00000097	Textiles - Products			MAN	2026-04-15 21:08:17.546202
318			Kirloskar Pneumatic Company Ltd	Kirl.Pneumatic	Company	INE811A01020		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000025	Compressors / Drilling Equipment			MAN	2026-04-15 21:08:17.546202
363			Maharashtra Scooters Ltd	Mah. Scooters	Company	INE288A01013		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:08:17.546202
466			Nath Industries Ltd	Nath Industries	Company	INE777A01023		Small Cap	00000044	Paper	00000064	Paper			MAN	2026-04-15 21:08:17.546202
148			EID Parry (India) Ltd	EID Parry	Company	INE126A01031		Small Cap	00000059	Sugar	00000088	Sugar			MAN	2026-04-15 21:08:56.561419
616			Hindustan Hardy Ltd	Hindustan Hardy	Company	INE724D01011		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:17.546202
823			Austin Engineering Company Ltd	Austin Engg Co	Company	INE759F01012		Small Cap	00000066	Bearings	00000013	Bearings			MAN	2026-04-15 21:08:17.546202
226			Gujarat State Fertilizers & Chemicals Ltd	G S F C	Company	INE026A01025		Small Cap	00000025	Fertilizers	00000049	Fertilizers			MAN	2026-04-15 21:08:56.561419
251			HEG Ltd	HEG	Company	INE545A01024		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000040	Electrodes - Graphites			MAN	2026-04-15 21:08:56.561419
503			Seshasayee Paper & Boards Ltd	Seshasayee Paper	Company	INE630A01024		Small Cap	00000044	Paper	00000064	Paper			MAN	2026-04-15 21:11:22.295015
525			Southern Petrochemicals Industries Corporation Ltd	S P I C	Company	INE147A01011		Small Cap	00000025	Fertilizers	00000049	Fertilizers			MAN	2026-04-15 21:11:22.295015
526			TVS Srichakra Ltd	TVS Srichakra	Company	INE421C01016		Small Cap	00000065	Tyres	00000105	Tyres			MAN	2026-04-15 21:11:22.295015
658			Grauer & Weil (India) Ltd	Grauer & Weil	Company	INE266D01021		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:11:22.295015
516			Siyaram Silk Mills Ltd	Siyaram Silk	Company	INE076B01028		Small Cap	00000062	Textiles	00000096	Textiles - Processing			MAN	2026-04-15 21:08:24.840641
541			Supreme Industries Ltd	Supreme Inds.	Company	INE195A01028		Mid Cap	00000048	Plastic products	00000075	Plastics Products			MAN	2026-04-15 21:08:24.840641
764			Triveni Glass Ltd	Triveni Glass	Company	INE094C01011		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:08:24.840641
971			Shree Rajasthan Syntex Ltd	Sh. Rajas. Synt.	Company	INE796C01011		Small Cap	00000062	Textiles	00000099	Textiles - Spinning - Synthetic / Blended			MAN	2026-04-15 21:08:24.840641
484			Rollatainers Ltd	Rollatainers	Company	INE927A01040		Small Cap	00000042	Packaging	00000062	Packaging			MAN	2026-04-15 21:08:52.876314
149			Procter & Gamble Health Ltd	P & G Health Ltd	Company	INE199A01012		Small Cap	00000046	Pharmaceuticals	00000073	Pharmaceuticals - Multinational			MAN	2026-04-15 21:11:12.625828
544			SML Mahindra Ltd	SML Mahindra	Company	INE294B01019		Small Cap	00000005	Automobile	00000005	Automobiles - LCVs / HCVs			MAN	2026-04-15 21:08:52.876314
76			Bimetal Bearings Ltd	Bimetal Bearings	Company	INE469A01019		Small Cap	00000066	Bearings	00000013	Bearings			MAN	2026-04-15 21:08:47.550441
554			Tata Power Company Ltd	Tata Power Co.	Company	INE245A01021		Large Cap	00000049	Power Generation & Distribution	00000076	Power Generation And Supply			MAN	2026-04-15 21:08:52.876314
610			Kennametal India Ltd	Kennametal India	Company	INE717A01029		Small Cap	00000010	Castings, Forgings & Fastners	00000017	Castings & Forgings			MAN	2026-04-15 21:08:52.876314
782			Lumax Industries Ltd	Lumax Industries	Company	INE162B01018		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:52.876314
58			Baroda Rayon Corporation Ltd	Baroda Rayon	Company	INE461A01024		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:09:03.148339
101			Caprihans India Ltd	Caprihans India	Company	INE479A01018		Small Cap	00000048	Plastic products	00000075	Plastics Products			MAN	2026-04-15 21:11:12.625828
25			Aravali Securities & Finance Ltd	Aravali Sec	Company	INE068C01015		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:08:37.986885
77			Birla Corporation Ltd	Birla Corpn.	Company	INE340A01012		Small Cap	00000011	Cement	00000018	Cement - North India			MAN	2026-04-15 21:08:37.986885
56			Ballarpur Industries Ltd	Ballarpur Inds.	Company	INE294A01037		Small Cap	00000044	Paper	00000064	Paper			MAN	2026-04-15 21:08:42.035142
114			Cipla Ltd	Cipla	Company	INE059A01026		Mid Cap	00000046	Pharmaceuticals	00000070	Pharmaceuticals - Indian - Bulk Drugs & Formln			MAN	2026-04-15 21:08:37.986885
92			Borosil Renewables Ltd	Borosil Renew.	Company	INE666D01022		Small Cap	00000029	Glass & Glass Products	00000055	Glass & Glass Products			MAN	2026-04-15 21:08:47.550441
186			Gammon India Ltd	Gammon India	Company	INE259B01020		Small Cap	00000032	Infrastructure Developers & Operators	00000031	Construction			MAN	2026-04-15 21:08:37.986885
268			Indag Rubber Ltd	Indag Rubber	Company	INE802D01023		Small Cap	00000047	Plantation & Plantation Products	00000106	Miscellaneous			MAN	2026-04-15 21:08:37.986885
283			Indian Hume Pipe Company Ltd	Indian Hume Pipe	Company	INE323C01030		Small Cap	00000032	Infrastructure Developers & Operators	00000031	Construction			MAN	2026-04-15 21:08:37.986885
315			Jyoti Ltd	Jyoti	Company	INE511D01012		Small Cap	00000008	Capital Goods - Electrical Equipment	00000039	Electric Equipment			MAN	2026-04-15 21:08:37.986885
125			CG Power & Industrial Solutions Ltd	CG Power & Ind	Company	INE067A01029		Large Cap	00000008	Capital Goods - Electrical Equipment	00000039	Electric Equipment			MAN	2026-04-15 21:08:47.550441
166			Excel Industries Ltd	Excel Industries	Company	INE369A01029		Small Cap	00000001	Agro Chemicals	00000067	Pesticides / Agrochemicals - Indian			MAN	2026-04-15 21:11:12.625828
167			FGP Ltd	FGP	Company	INE512A01016		Small Cap	00000039	Miscellaneous	00000106	Miscellaneous			MAN	2026-04-15 21:11:12.625828
278			Indian Card Clothing Company Ltd	Indian CardCloth	Company	INE061A01014		Small Cap	00000062	Textiles	00000097	Textiles - Products			MAN	2026-04-15 21:11:12.625828
128			DCM Ltd	DCM	Company	INE498A01018		Small Cap	00000010	Castings, Forgings & Fastners	00000017	Castings & Forgings			MAN	2026-04-15 21:08:47.550441
316			K C P Ltd	K C P	Company	INE805C01028		Small Cap	00000011	Cement	00000019	Cement - South India			MAN	2026-04-15 21:11:12.625828
136			Deepak Nitrite Ltd	Deepak Nitrite	Company	INE288B01029		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:08:47.550441
336			Kirloskar Brothers Ltd	Kirl. Brothers	Company	INE732A01036		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000078	Pumps			MAN	2026-04-15 21:11:12.625828
214			Setco Automotive Ltd	Setco Automotive	Company	INE878E01021		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:47.550441
465			Rallis India Ltd	Rallis India	Company	INE613A01020		Small Cap	00000001	Agro Chemicals	00000067	Pesticides / Agrochemicals - Indian			MAN	2026-04-15 21:11:12.625828
474			Raymond Ltd	Raymond	Company	INE301A01014		Small Cap	00000002	Air Transport Service	00000103	Transport - Airlines			MAN	2026-04-15 21:11:12.625828
291			Panasonic Carbon India Company Ltd	Panasonic Carbon	Company	INE013E01017		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000040	Electrodes - Graphites			MAN	2026-04-15 21:08:47.550441
347			Lakshmi Precision Screws Ltd	Lak. Prec. Screw	Company	INE651C01018		Small Cap	00000010	Castings, Forgings & Fastners	00000048	Fasteners			MAN	2026-04-15 21:08:47.550441
480			Procter & Gamble Hygiene and Health Care Ltd	P & G Hygiene	Company	INE179A01014		Small Cap	00000027	FMCG	00000066	Personal Care - Multinational			MAN	2026-04-15 21:08:47.550441
337			Cummins India Ltd	Cummins India	Company	INE298A01020		Large Cap	00000009	Capital Goods-Non Electrical Equipment	00000046	Engines			MAN	2026-04-15 21:08:37.986885
370			MPIL Corporation Ltd	MPIL Corporation	Company	INE844C01027		Small Cap	00000039	Miscellaneous	00000106	Miscellaneous			MAN	2026-04-15 21:08:37.986885
391			MRF Ltd	MRF	Company	INE883A01011		Mid Cap	00000065	Tyres	00000105	Tyres			MAN	2026-04-15 21:08:37.986885
67			Bhagawati Oxygen Ltd	Bhagawati Oxygen	Company	INE026I01010		Small Cap	00000028	Gas Distribution	00000106	Miscellaneous			MAN	2026-04-15 21:08:42.035142
301			ITC Ltd	ITC	Company	INE154A01025		Large Cap	00000063	Tobacco Products	00000024	Cigarettes			MAN	2026-04-15 21:09:03.148339
342			KSB Ltd	KSB	Company	INE999A01023		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000078	Pumps			MAN	2026-04-15 21:09:03.148339
6			ACC Ltd	ACC	Company	INE012A01025		Small Cap	00000011	Cement	00000018	Cement - North India			MAN	2026-04-15 21:08:41.309362
152			Elecon Engineering Company Ltd	Elecon Engg.Co	Company	INE205B01031		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000044	Engineering			MAN	2026-04-15 21:08:41.309362
535			JK Lakshmi Cement Ltd	JK Lakshmi Cem.	Company	INE786A01032		Small Cap	00000011	Cement	00000018	Cement - North India			MAN	2026-04-15 21:11:12.625828
64			Berger Paints India Ltd	Berger Paints	Company	INE463A01038		Mid Cap	00000043	Paints/Varnish	00000063	Paints / Varnishes			MAN	2026-04-15 21:05:40.45615
88			Reliance Infrastructure Ltd	Reliance Infra.	Company	INE036A01016		Small Cap	00000032	Infrastructure Developers & Operators	00000045	Engineering - Turnkey Services			MAN	2026-04-15 21:05:40.45615
539			Sundram Fasteners Ltd	Sundram Fasten.	Company	INE387A01021		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:59.842413
677			Lyka Labs Ltd	Lyka Labs	Company	INE933A01014		Small Cap	00000046	Pharmaceuticals	00000070	Pharmaceuticals - Indian - Bulk Drugs & Formln			MAN	2026-04-15 21:08:41.309362
213			Gujarat Narmada Valley Fertilizers & Chemicals Ltd	G N F C	Company	INE113A01013		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:05:40.45615
69			Bharat Forge Ltd	Bharat Forge	Company	INE465A01025		Mid Cap	00000010	Castings, Forgings & Fastners	00000017	Castings & Forgings			MAN	2026-04-15 21:09:03.148339
265			GOCL Corporation Ltd	GOCL Corpn.	Company	INE077F01035		Small Cap	00000021	Diversified	00000109	Diversified - Medium / Small			MAN	2026-04-15 21:08:41.309362
84			The Bombay Burmah Trading Corporation Ltd	The Bombay Burmah	Company	INE050A01025		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:09:03.148339
105			Cemindia Projects Ltd	Cemindia Project	Company	INE686A01026		Small Cap	00000032	Infrastructure Developers & Operators	00000031	Construction			MAN	2026-04-15 21:09:03.148339
253			Hindustan Composites Ltd	Hind.Composites	Company	INE310C01029		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:09:03.148339
383			Modella Woollens Ltd	Modella Woollens	Company	INE380D01012		Small Cap	00000064	Trading	00000101	Trading			MAN	2026-04-15 21:09:03.148339
233			Harrisons Malayalam Ltd	Harri. Malayalam	Company	INE544A01019		Small Cap	00000047	Plantation & Plantation Products	00000106	Miscellaneous			MAN	2026-04-15 21:05:40.45615
242			ABB India Ltd	A B B	Company	INE117A01022		Large Cap	00000008	Capital Goods - Electrical Equipment	00000039	Electric Equipment			MAN	2026-04-15 21:05:40.45615
262			Zensar Technologies Ltd	Zensar Tech.	Company	INE520A01027		Small Cap	00000034	IT - Software	00000028	Computers - Software - Medium / Small			MAN	2026-04-15 21:05:40.45615
107			Aditya Birla Real Estate Ltd	A B Real Estate	Company	INE055A01016		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:08:42.035142
112			Chowgule Steamships Ltd	Chowgule Steam	Company	INE490A01015		Small Cap	00000056	Shipping	00000082	Shipping			MAN	2026-04-15 21:08:42.035142
131			Dalmia Bharat Sugar & Industries Ltd	Dalmia Bharat	Company	INE495A01022		Small Cap	00000059	Sugar	00000088	Sugar			MAN	2026-04-15 21:08:42.035142
165			Everest Industries Ltd	Everest Inds.	Company	INE295A01018		Small Cap	00000012	Cement - Products	00000020	Cement Products			MAN	2026-04-15 21:08:42.035142
470			Rane Holdings Ltd	Rane Holdings	Company	INE384A01010		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:05:40.45615
326			Keltech Energies Ltd	Keltech Energies	Company	INE881E01017		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:08:41.309362
346			LMW Ltd	LMW	Company	INE269B01029		Small Cap	00000072	Engineering	00000092	Textile Machinery			MAN	2026-04-15 21:08:41.309362
198			Forbes & Company Ltd	Forbes & Co	Company	INE518A01013		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:08:42.035142
534			Stovec Industries Ltd	Stovec Inds.	Company	INE755D01015		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000092	Textile Machinery			MAN	2026-04-15 21:08:41.309362
243			Novartis India Ltd	Novartis India	Company	INE234A01025		Small Cap	00000046	Pharmaceuticals	00000073	Pharmaceuticals - Multinational			MAN	2026-04-15 21:08:42.035142
753			National Standard (India) Ltd	National Standar	Company	INE166R01015		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:08:41.309362
512			Shri Dinesh Mills Ltd	Sh. Dinesh Mills	Company	INE204C01024		Small Cap	00000062	Textiles	00000097	Textiles - Products			MAN	2026-04-15 21:05:40.45615
558			Tayo Rolls Ltd	Tayo Rolls	Company	INE895C01011		Small Cap	00000010	Castings, Forgings & Fastners	00000017	Castings & Forgings			MAN	2026-04-15 21:09:03.148339
529			Standard Industries Ltd	Standard Inds.	Company	INE173A01025		Small Cap	00000062	Textiles	00000101	Trading			MAN	2026-04-15 21:05:40.45615
609			Wheels India Ltd	Wheels India	Company	INE715A01015		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:05:40.45615
574			Tuticorin Alkali Chemicals & Fertilizers Ltd	Tuticorin Alkali	Company	INE400A01014		Small Cap	00000014	Chemicals	00000023	Chlor Alkali / Soda Ash			MAN	2026-04-15 21:09:03.148339
770			The Peria Karamalai Tea & Produce Company Ltd	The Peria Karamalai Tea	Company	INE431F01018		Small Cap	00000047	Plantation & Plantation Products	00000089	Tea			MAN	2026-04-15 21:08:41.309362
495			Samtel (India) Ltd	Samtel (India)	Company	INE538C01017		Small Cap	00000017	Consumer Durables	00000043	Electronics - Components			MAN	2026-04-15 21:08:23.90416
159			GE Vernova T&D India Ltd	GE Vernova T&D	Company	INE200A01026		Large Cap	00000008	Capital Goods - Electrical Equipment	00000039	Electric Equipment			MAN	2026-04-15 21:11:16.103454
123			Coromandel International Ltd	Coromandel Inter	Company	INE169A01031		Mid Cap	00000025	Fertilizers	00000049	Fertilizers			MAN	2026-04-15 20:55:22.752241
197			Federal-Mogul Goetze (India) Ltd	Federal-Mogul Go	Company	INE529A01010		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 20:55:22.752241
415			NOCIL Ltd	NOCIL	Company	INE163A01018		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:08:56.561419
439			Amal Ltd	Amal	Company	INE841D01013		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:08:56.561419
201			Goodricke Group Ltd	Goodricke Group	Company	INE300A01016		Small Cap	00000047	Plantation & Plantation Products	00000089	Tea			MAN	2026-04-15 20:55:22.752241
217			Gujarat Alkalies & Chemicals Ltd	Gujarat Alkalies	Company	INE186A01019		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 20:55:22.752241
446			Schaeffler India Ltd	Schaeffler India	Company	INE513A01022		Mid Cap	00000066	Bearings	00000013	Bearings			MAN	2026-04-15 21:08:56.561419
598			VST Industries Ltd	VST Industries	Company	INE710A01016		Small Cap	00000063	Tobacco Products	00000024	Cigarettes			MAN	2026-04-15 21:08:56.561419
537			Sundaram Brake Linings Ltd	Sundaram Brake	Company	INE073D01013		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:23.90416
597			Voltas Ltd	Voltas	Company	INE226A01021		Mid Cap	00000017	Consumer Durables	00000002	Air-conditioners			MAN	2026-04-15 21:08:23.90416
606			Wendt India Ltd	Wendt India	Company	INE274C01019		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000001	Abrasives And Grinding Wheels			MAN	2026-04-15 21:08:23.90416
62			Bayer CropScience Ltd	Bayer Crop Sci.	Company	INE462A01022		Small Cap	00000001	Agro Chemicals	00000068	Pesticides / Agrochemicals - Multinational			MAN	2026-04-15 21:11:16.103454
355			Manugraph India Ltd	Manugraph India	Company	INE867A01022		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000044	Engineering			MAN	2026-04-15 20:55:22.752241
800			Bajaj Steel Industries Ltd	Bajaj Steel Inds	Company	INE704G01024		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000044	Engineering			MAN	2026-04-15 21:08:41.309362
259			BirlaNu Ltd	BirlaNu Ltd	Company	INE557A01011		Small Cap	00000012	Cement - Products	00000020	Cement Products			MAN	2026-04-15 21:08:42.035142
538			TVS Holdings Ltd	TVS Holdings	Company	INE105A01035		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 20:55:22.752241
576			Ultramarine & Pigments Ltd	Ultramarine Pig.	Company	INE405A01021		Small Cap	00000014	Chemicals	00000038	Dyes And Pigments			MAN	2026-04-15 20:55:22.752241
311			Jay Shree Tea & Industries Ltd	Jay Shree Tea	Company	INE364A01020		Small Cap	00000047	Plantation & Plantation Products	00000089	Tea			MAN	2026-04-15 21:08:42.035142
594			Chrome Silicon Ltd	Chrome Silicon	Company	INE114E01013		Small Cap	00000071	Ferro Alloys	00000059	Mining / Minerals / Metals			MAN	2026-04-15 20:55:22.752241
619			Z F Steering Gear (India) Ltd	Z F Steering	Company	INE116C01012		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 20:55:22.752241
82			Blue Star Ltd	Blue Star	Company	INE472A01039		Mid Cap	00000017	Consumer Durables	00000002	Air-conditioners			MAN	2026-04-15 21:11:16.103454
99			CESC Ltd	CESC	Company	INE486A01021		Small Cap	00000049	Power Generation & Distribution	00000076	Power Generation And Supply			MAN	2026-04-15 21:11:16.103454
209			Grindwell Norton Ltd	Grindwell Norton	Company	INE536A01023		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000001	Abrasives And Grinding Wheels			MAN	2026-04-15 21:11:16.103454
307			Jagatjit Industries Ltd	Jagatjit Inds.	Company	INE574A01016		Small Cap	00000003	Alcoholic Beverages	00000014	Breweries & Distilleries			MAN	2026-04-15 21:11:16.103454
398			Mysore Petro Chemicals Ltd	Mysore Petro	Company	INE741A01011		Small Cap	00000014	Chemicals	00000101	Trading			MAN	2026-04-15 21:11:16.103454
607			West Coast Paper Mills Ltd	West Coast Paper	Company	INE976A01021		Small Cap	00000044	Paper	00000064	Paper			MAN	2026-04-15 21:11:16.103454
628			Skyline Millars Ltd	Skyline Millars	Company	INE178E01026		Small Cap	00000051	Realty	00000031	Construction			MAN	2026-04-15 21:11:16.103454
199			Goodyear India Ltd	Goodyear India	Company	INE533A01012		Small Cap	00000065	Tyres	00000105	Tyres			MAN	2026-04-15 21:08:23.90416
250			AGI Greenpac Ltd	AGI Greenpac	Company	INE415A01038		Small Cap	00000042	Packaging	00000062	Packaging			MAN	2026-04-15 21:08:23.90416
641			Oswal Green Tech Ltd	Oswal Green Tech	Company	INE143A01010		Small Cap	00000021	Diversified	00000109	Diversified - Medium / Small			MAN	2026-04-15 21:11:16.103454
282			Indian Hotels Co Ltd	Indian Hotels Co	Company	INE053A01029		Mid Cap	00000031	Hotels & Restaurants	00000057	Hotels			MAN	2026-04-15 21:08:23.90416
608			Western Ministil Ltd	Western Ministil	Company	INE187U01015		Small Cap	00000057	Steel	00000085	Steel - Medium / Small			MAN	2026-04-15 21:08:23.90416
611			Willard India Ltd	Willard India	Company	INE481D01018		Small Cap	00000059	Sugar	00000088	Sugar			MAN	2026-04-15 21:08:23.90416
60			Bata India Ltd	Bata India	Company	INE176A01028		Small Cap	00000035	Leather	00000058	Leather / Leather Products			MAN	2026-04-15 21:08:33.406695
824			Disa India Ltd	Disa India	Company	INE131C01011		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000044	Engineering			MAN	2026-04-15 21:08:17.546202
111			Exide Industries Ltd	Exide Inds.	Company	INE302A01020		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:33.406695
237			Hero MotoCorp Ltd	Hero Motocorp	Company	INE158A01026		Large Cap	00000005	Automobile	00000009	Automobiles - Motorcycles / Mopeds			MAN	2026-04-15 21:08:33.406695
557			Tata Consumer Products Ltd	Tata Consumer	Company	INE192A01025		Large Cap	00000047	Plantation & Plantation Products	00000089	Tea			MAN	2026-04-15 21:04:44.029696
33			Asian Hotels (North) Ltd	Asian Hotels (N)	Company	INE363A01022		Small Cap	00000031	Hotels & Restaurants	00000057	Hotels			MAN	2026-04-15 21:08:53.797022
519			Somany Ceramics Ltd	Somany Ceramics	Company	INE355A01028		Small Cap	00000013	Ceramic Products	00000021	Ceramics - Tiles / Sanitaryware			MAN	2026-04-15 21:09:01.477119
287			Singer India Ltd	Singer India	Company	INE638A01035		Small Cap	00000072	Engineering	00000044	Engineering			MAN	2026-04-15 21:08:33.406695
381			Milkfood Ltd	Milkfood	Company	INE588G01021		Small Cap	00000027	FMCG	00000053	Food - Processing - Indian			MAN	2026-04-15 21:08:33.406695
385			Lords Chloro Alkali Ltd	Lords Chloro	Company	INE846D01012		Small Cap	00000014	Chemicals	00000023	Chlor Alkali / Soda Ash			MAN	2026-04-15 21:08:33.406695
14			Ambalal Sarabhai Enterprises Ltd	Ambalal Sarabhai	Company	INE432A01017		Small Cap	00000046	Pharmaceuticals	00000101	Trading			MAN	2026-04-15 21:11:20.503748
410			NELCO Ltd	NELCO	Company	INE045B01015		Small Cap	00000060	Telecom Equipment & Infra Services	00000090	Telecommunications - Service Provider			MAN	2026-04-15 21:08:33.406695
20			Andhra Paper Ltd	Andhra Paper	Company	INE435A01051		Small Cap	00000044	Paper	00000064	Paper			MAN	2026-04-15 21:11:20.503748
81			Blue Blends (India) Ltd	Blue Blends (I)	Company	INE113O01014		Small Cap	00000062	Textiles	00000096	Textiles - Processing			MAN	2026-04-15 21:11:20.503748
622			Automotive Axles Ltd	Automotive Axles	Company	INE449A01011		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:08:23.90416
505			Shalimar Paints Ltd	Shalimar Paints	Company	INE849C01026		Small Cap	00000043	Paints/Varnish	00000063	Paints / Varnishes			MAN	2026-04-15 21:08:33.406695
85			Bombay Dyeing & Manufacturing Company Ltd	Bombay Dyeing	Company	INE032A01023		Small Cap	00000062	Textiles	00000095	Textiles - Manmade			MAN	2026-04-15 21:11:20.503748
302			IVP Ltd	IVP	Company	INE043C01018		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:11:20.503748
577			Unichem Laboratories Ltd	Unichem Labs.	Company	INE351A01035		Small Cap	00000046	Pharmaceuticals	00000070	Pharmaceuticals - Indian - Bulk Drugs & Formln			MAN	2026-04-15 21:08:33.406695
9			Aegis Logistics Ltd	Aegis Logistics	Company	INE208C01025		Small Cap	00000064	Trading	00000101	Trading			MAN	2026-04-15 21:04:44.029696
11			Alfred Herbert (India) Ltd	Alfred Herbert	Company	INE782D01027		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:04:44.029696
142			Diamines & Chemicals Ltd	Diamines & Chem.	Company	INE591D01014		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:04:44.029696
340			Kothari Industrial Corporation Ltd	Kothari Indl	Company	INE972A01020		Small Cap	00000064	Trading	00000101	Trading			MAN	2026-04-15 21:04:44.029696
352			M M Rubber Co Ltd	M M Rubber	Company	INE159E01026		Small Cap	00000047	Plantation & Plantation Products	00000106	Miscellaneous			MAN	2026-04-15 21:04:44.029696
420			Orient Paper & Industries Ltd	Orient Paper	Company	INE592A01026		Small Cap	00000044	Paper	00000064	Paper			MAN	2026-04-15 21:04:44.029696
53			Force Motors Ltd	Force Motors	Company	INE451A01017		Small Cap	00000005	Automobile	00000005	Automobiles - LCVs / HCVs			MAN	2026-04-15 21:08:53.797022
55			Balkrishna Industries Ltd	Balkrishna Inds	Company	INE787D01026		Mid Cap	00000065	Tyres	00000105	Tyres			MAN	2026-04-15 21:08:53.797022
57			Bannari Amman Sugars Ltd	Bannari Amm.Sug.	Company	INE459A01010		Small Cap	00000059	Sugar	00000088	Sugar			MAN	2026-04-15 21:08:53.797022
59			BASF India Ltd	BASF India	Company	INE373A01013		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:08:53.797022
119			Sudarshan Colorants India Ltd	Sudarshan Colorants	Company	INE492A01029		Small Cap	00000014	Chemicals	00000038	Dyes And Pigments			MAN	2026-04-15 21:08:53.797022
137			Deltron Ltd	Deltron	Company	INE272R01011		Small Cap	00000017	Consumer Durables	00000043	Electronics - Components			MAN	2026-04-15 21:08:53.797022
189			Garware Hi Tech Films Ltd	Garware Hi Tech	Company	INE291A01017		Small Cap	00000042	Packaging	00000062	Packaging			MAN	2026-04-15 21:08:53.797022
204			Saregama India Ltd	Saregama India	Company	INE979A01025		Small Cap	00000024	Entertainment	00000047	Entertainment / Electronic Media Software			MAN	2026-04-15 21:08:53.797022
587			Universal Cables Ltd	Universal Cables	Company	INE279A01012		Small Cap	00000007	Cables	00000015	Cables - Power			MAN	2026-04-15 21:04:44.029696
614			Wipro Ltd	Wipro	Company	INE075A01022		Large Cap	00000034	IT - Software	00000027	Computers - Software - Large			MAN	2026-04-15 21:04:44.029696
620			Zenith Steel Pipes & Industries Ltd	Zenith Steel	Company	INE318D01020		Small Cap	00000057	Steel	00000085	Steel - Medium / Small			MAN	2026-04-15 21:04:44.029696
118			Colgate-Palmolive (India) Ltd	Colgate-Palmoliv	Company	INE259A01022		Mid Cap	00000027	FMCG	00000066	Personal Care - Multinational			MAN	2026-04-15 21:08:51.984819
1024			Asahi India Glass Ltd	Asahi India Glas	Company	INE439A01020		Small Cap	00000029	Glass & Glass Products	00000055	Glass & Glass Products			MAN	2026-04-15 21:05:24.685078
151			Eicher Motors Ltd	Eicher Motors	Company	INE066A01021		Large Cap	00000005	Automobile	00000009	Automobiles - Motorcycles / Mopeds			MAN	2026-04-15 21:08:51.984819
164			EPL Ltd	EPL Ltd	Company	INE255A01020		Small Cap	00000042	Packaging	00000062	Packaging			MAN	2026-04-15 21:08:51.984819
180			GKW Ltd	GKW	Company	INE528A01020		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:08:51.984819
200			Kansai Nerolac Paints Ltd	Kansai Nerolac	Company	INE531A01024		Small Cap	00000043	Paints/Varnish	00000063	Paints / Varnishes			MAN	2026-04-15 21:11:20.503748
1131			Honeywell Automation India Ltd	Honeywell Auto	Company	INE671A01010		Small Cap	00000076	Electronics	00000043	Electronics - Components			MAN	2026-04-15 21:05:24.685078
1149			Jamna Auto Industries Ltd	Jamna Auto Inds.	Company	INE039C01032		Small Cap	00000004	Auto Ancillaries	00000010	Auto Ancillaries			MAN	2026-04-15 21:05:24.685078
227			Grasim Industries Ltd	Grasim Inds	Company	INE047A01021		Large Cap	00000062	Textiles	00000095	Textiles - Manmade			MAN	2026-04-15 21:08:51.984819
256			Sanofi India Ltd	Sanofi India	Company	INE058A01010		Small Cap	00000046	Pharmaceuticals	00000073	Pharmaceuticals - Multinational			MAN	2026-04-15 21:08:51.984819
478			Semac Construction Ltd	Semac Construct	Company	INE617A01013		Small Cap	00000032	Infrastructure Developers & Operators	00000045	Engineering - Turnkey Services			MAN	2026-04-15 21:08:51.984819
602			Walchandnagar Industries Ltd	Walchan. Inds.	Company	INE711A01022		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000044	Engineering			MAN	2026-04-15 21:08:51.984819
255			Hindustan Unilever Ltd	Hind. Unilever	Company	INE030A01027		Large Cap	00000027	FMCG	00000066	Personal Care - Multinational			MAN	2026-04-15 21:08:53.797022
207			Great Eastern Shipping Company Ltd	GE Shipping Co	Company	INE017A01032		Small Cap	00000056	Shipping	00000082	Shipping			MAN	2026-04-15 21:11:20.503748
252			Hindalco Industries Ltd	Hindalco Inds.	Company	INE038A01020		Large Cap	00000040	Non Ferrous Metals	00000003	Aluminium and Aluminium Products			MAN	2026-04-15 21:11:20.503748
290			Indo Gulf Industries Ltd	Indo Gulf Inds.	Company	INE684U01011		Small Cap	00000014	Chemicals	00000022	Chemicals			MAN	2026-04-15 21:11:20.503748
103			Carborundum Universal Ltd	Carborundum Uni.	Company	INE120A01034		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000001	Abrasives And Grinding Wheels			MAN	2026-04-15 21:05:24.685078
196			Goa Carbon Ltd	Goa Carbon	Company	INE426D01013		Small Cap	00000045	Petrochemicals	00000069	Petrochemicals			MAN	2026-04-15 21:05:24.685078
216			Gujarat Themis Biosyn Ltd	Guj. Themis Bio.	Company	INE942C01045		Small Cap	00000046	Pharmaceuticals	00000071	Pharmaceuticals - Indian - Bulk Drugs			MAN	2026-04-15 21:05:24.685078
328			Whirlpool of India Ltd	Whirlpool India	Company	INE716A01013		Small Cap	00000017	Consumer Durables	00000036	Domestic Appliances			MAN	2026-04-15 21:05:24.685078
367			Mangalam Cement Ltd	Mangalam Cement	Company	INE347A01017		Small Cap	00000011	Cement	00000018	Cement - North India			MAN	2026-04-15 21:05:24.685078
493			Signature Green Corporation Ltd	Signature Green	Company	INE131O01024		Small Cap	00000023	Edible Oil	00000083	Solvent Extraction			MAN	2026-04-15 21:05:24.685078
650			Delton Cables Ltd	Delton Cables	Company	INE872E01016		Small Cap	00000007	Cables	00000015	Cables - Power			MAN	2026-04-15 20:55:22.752241
502			Vedanta Ltd	Vedanta	Company	INE205A01025		Large Cap	00000038	Mining & Mineral products	00000059	Mining / Minerals / Metals			MAN	2026-04-15 21:08:47.550441
514			Siemens Ltd	Siemens	Company	INE003A01024		Large Cap	00000008	Capital Goods - Electrical Equipment	00000039	Electric Equipment			MAN	2026-04-15 21:11:20.503748
51			Bajaj Electricals Ltd	Bajaj Electrical	Company	INE193E01025		Small Cap	00000017	Consumer Durables	00000036	Domestic Appliances			MAN	2026-04-15 21:08:51.984819
61			Batliboi Ltd	Batliboi	Company	INE177C01022		Small Cap	00000009	Capital Goods-Non Electrical Equipment	00000044	Engineering			MAN	2026-04-15 21:08:51.984819
964			Sakthi Finance Ltd	Sakthi Finance	Company	INE302E01014		Small Cap	00000026	Finance	00000050	Finance & Investments			FIN	2026-04-15 21:05:24.685078
401			NCL Industries Ltd	NCL Industries	Company	INE732C01016		Small Cap	00000011	Cement	00000019	Cement - South India			MAN	2026-04-15 21:11:25.111663
\.


--
-- Data for Name: company_master; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.company_master (co_code, bsecode, nsesymbol, companyname, companyshortname, categoryname, isin, bsegroup, mcaptype, sectorcode, sectorname, industrycode, industryname, bselistedflag, nselistedflag, displaytype, updated_at) FROM stdin;
\.


--
-- Data for Name: sync_meta; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.sync_meta (key, value) FROM stdin;
last_synced_page	488
\.


--
-- Name: companies companies_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.companies
    ADD CONSTRAINT companies_pkey PRIMARY KEY (co_code);


--
-- Name: company_master company_master_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.company_master
    ADD CONSTRAINT company_master_pkey PRIMARY KEY (co_code);


--
-- Name: sync_meta sync_meta_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.sync_meta
    ADD CONSTRAINT sync_meta_pkey PRIMARY KEY (key);


--
-- Name: idx_bsecode; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_bsecode ON public.company_master USING btree (bsecode);


--
-- Name: idx_companies_bsecode; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_companies_bsecode ON public.companies USING btree (bsecode);


--
-- Name: idx_companies_isin; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_companies_isin ON public.companies USING btree (isin);


--
-- Name: idx_companies_name_trgm; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_companies_name_trgm ON public.companies USING gin (companyname public.gin_trgm_ops);


--
-- Name: idx_companies_nsesymbol; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_companies_nsesymbol ON public.companies USING btree (nsesymbol);


--
-- Name: idx_companies_shortname_trgm; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_companies_shortname_trgm ON public.companies USING gin (companyshortname public.gin_trgm_ops);


--
-- Name: idx_companyname; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_companyname ON public.company_master USING btree (upper((companyname)::text));


--
-- Name: idx_nsesymbol; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_nsesymbol ON public.company_master USING btree (upper((nsesymbol)::text));


--
-- PostgreSQL database dump complete
--

\unrestrict 0f1UvOsnZEokuRl7BJeIU6dyjSqbeniB2wOCCKTwuyW8gneSuBswHzHGUcDv9Ef

