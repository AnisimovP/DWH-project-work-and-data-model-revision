drop table if exists  shipping_country_rates cascade;

create table shipping_country_rates (
	countryratessid					serial not NULL,
	shipping_country 				text null,
	shipping_country_base_rate		numeric(14, 3) null,
	primary key (countryratessid)
);

insert into shipping_country_rates(shipping_country, shipping_country_base_rate)

select  distinct shipping_country,
		shipping_country_base_rate
from shipping s
;

drop table if exists shipping_agreement cascade;

create table shipping_agreement (
	agreementid 					bigint not null,
	agreement_number				text null,
	agreement_rate					numeric(14, 3) null,
	agreement_commission			numeric(14, 3) null,
	primary key (agreementid)
);

insert into shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)

select  distinct(regexp_split_to_array(vendor_agreement_description, ':+'))[1]::bigint as agreementid,
		(regexp_split_to_array(vendor_agreement_description, ':+'))[2]::text as agreement_number,
		(regexp_split_to_array(vendor_agreement_description, ':+'))[3]::numeric(14, 3) as agreement_rate,
		(regexp_split_to_array(vendor_agreement_description, ':+'))[4]::numeric(14, 3) as agreement_commission
from shipping
;

drop table if exists shipping_transfer  cascade;

create table shipping_transfer  (
	transferid 						serial not null,
	transfer_type 					text,
	transfer_model					text,
	shipping_transfer_rate 			numeric(14, 3) null,
	primary key(transferid)
);

insert into shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)

select  distinct(regexp_split_to_array(shipping_transfer_description, ':'))[1] as transfer_type,
		(regexp_split_to_array(shipping_transfer_description, ':'))[2] as transfer_model,
		shipping_transfer_rate
from shipping
order by transfer_type, transfer_model
;

drop table if exists shipping_info  cascade;

create table shipping_info (
	shippingid 						bigint,
	vendorid						bigint,
	payment_amount					numeric(14,3),
	shipping_plan_datetime			timestamp,
	countryratessid					bigint,
	agreementid						bigint,
	transferid						bigint,
	primary key (shippingid),
	foreign key (countryratessid) references shipping_country_rates (countryratessid) on update cascade,
	foreign key (agreementid) references shipping_agreement (agreementid) on update cascade,
	foreign key (transferid) references shipping_transfer (transferid) on update cascade
);

insert into shipping_info (shippingid, vendorid, payment_amount, shipping_plan_datetime, countryratessid, agreementid, transferid)

select  distinct s.shippingid,
		s.vendorid,
		s.payment_amount,
		s.shipping_plan_datetime,
		scr.countryratessid,
		sa.agreementid,
		st.transferid 
from(
		select  shippingid,
				vendorid,
				payment_amount,
				shipping_plan_datetime,
				shipping_country,
				(regexp_split_to_array(vendor_agreement_description, ':+'))[1]::int as agreementid,
				(regexp_split_to_array(shipping_transfer_description, ':'))[1] as transfer_type,
				(regexp_split_to_array(shipping_transfer_description, ':'))[2] as transfer_model
		from shipping
) as s
left join shipping_country_rates scr on s.shipping_country = scr.shipping_country
left join shipping_agreement sa on s.agreementid = sa.agreementid
left join shipping_transfer st on s.transfer_type = st.transfer_type and s.transfer_model = st.transfer_model
;

drop table if exists shipping_status cascade;

create table shipping_status (
	shippingid 						bigint,
	status							text,
	state							text,
	shipping_start_fact_datetime	timestamp,
	shipping_end_fact_datetime		timestamp,
	primary key (shippingid)
);

insert into shipping_status (shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)

with table1 as (
select 
		shippingid, 
		status, 
		state, 
		state_datetime,
		row_number () over(partition by shippingid order by state_datetime desc) as rn
from shipping),
table2 as (
select
		shippingid,
		state_datetime as shipping_start_fact_datetime
from shipping 
where state = 'booked'),
table3 as (
select
		shippingid,
		state_datetime as shipping_end_fact_datetime
from shipping 
where state = 'recieved')
select 	table1.shippingid, 
		table1.status, 
		table1.state,
		table2.shipping_start_fact_datetime,
		table3.shipping_end_fact_datetime
from table1
left join table2 on table1.shippingid = table2.shippingid
left join table3 on table1.shippingid = table3.shippingid
where table1.rn = 1
order by table1.shippingid
;


create or replace view shipping_datamart as

select 	si.shippingid,
		si.vendorid,
		st.transferid as transfer_type,
		date_trunc('day', (ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime))  as full_day_at_shipping,
		case 
			when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1
			else 0
		end as is_delay,
		case 
			when ss.status = 'finished' then 1
			else 0
		end as is_shipping_finish,
		case 
			when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then coalesce(DATE_PART('day', AGE(ss.shipping_end_fact_datetime, si.shipping_plan_datetime)),0) 
			else 0
		end as delay_day_at_shipping,
		si.payment_amount,
		si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) as vat,
		si.payment_amount * sa.agreement_commission as profit
from shipping_info si
left join shipping_transfer st on si.transferid = st.transferid
left join shipping_status ss on si.shippingid = ss.shippingid 
left join shipping_country_rates scr on si.countryratessid = scr.countryratessid
left join shipping_agreement sa on si.agreementid = sa.agreementid
order by si.shippingid;