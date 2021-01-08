-- Сколько по месяцам зарабатывали аффилиаты

with ref_mod as
(
select ai1.marker
        , ai2.marker as parent_marker
        , case when ai2.marker is NULL then 0 else 1 end as flag_child
        , to_date(ai1.created_at) as child_reg_date
        , to_date(ai2.created_at) as parent_reg_date
from tp.users u
    inner join tp.affiliate_infos ai1 on ai1.affiliate_id = u.id and ai1.internal = 0
    left join tp.affiliate_infos ai2 on ai2.affiliate_id = u.parent_id and ai2.internal = 0
)


select campaign_id
        , report_month
        
        , count(distinct case when parent_marker is not NULL then marker end) as children_affs
        , count(distinct case when parent_marker is NULL then marker end) as non_children_affs
        , count(distinct marker) as affs_with_revenue
        , count(distinct parent_marker) as parents_affs
        
        , sum(case when parent_marker is not NULL then zeroifnull(bookings) else 0 end) as children_bookings
        , sum(case when parent_marker is NULL then zeroifnull(bookings) else 0 end) as non_children_bookings
        , sum(zeroifnull(bookings)) as bookings
        
        , sum(case when parent_marker is not NULL then zeroifnull(action_profit) else 0 end) as children_profit
        , sum(case when parent_marker is NULL then zeroifnull(action_profit) else 0 end) as non_children_profit
        , sum(zeroifnull(action_profit)) as profit
        
        , sum(case when parent_marker is not NULL then zeroifnull(affiliate_commission) else 0 end) as children_commission
        , sum(case when parent_marker is NULL then zeroifnull(affiliate_commission) else 0 end) as non_children_commission
        , sum(zeroifnull(affiliate_commission)) as commission
from
(
select la.marker
        , la.campaign_id
        , date_trunc('month',la.booked_at) as report_month
        , rm.parent_marker
        , count(distinct la.internal_id) as bookings
        , sum(zeroifnull(la.action_profit)*la.rate_usd)/100 as action_profit
        , sum(zeroifnull(la.affiliate_commission)*la.rate_usd)/100 as affiliate_commission
from tp.lombard_actions la
        inner join ref_mod rm on rm.marker = la.marker
where date_trunc('month',la.booked_at) >= '2014-01-01'
        and la.action_state in ('paid')
        and la.type='action'
        and la.action_type = 'booking'
        and la.campaign_id not in(0)
group by 1,2,3,4
) a
GROUP BY 1,2


-- Сколько мы выплачивали по месяцам по программе сала и лука:

with ref_mod as
(
select ai1.marker
        , ai2.marker as parent_marker
        , case when ai2.marker is NULL then 0 else 1 end as flag_child
        , to_date(ai1.created_at) as child_reg_date
        , to_date(ai2.created_at) as parent_reg_date
from tp.users u
    inner join tp.affiliate_infos ai1 on ai1.affiliate_id = u.id and ai1.internal = 0
    left join tp.affiliate_infos ai2 on ai2.affiliate_id = u.parent_id and ai2.internal = 0
)


select campaign_id
        , report_month
        
        , sum(zeroifnull(referral_payouts)) as referral_payouts
        , sum(zeroifnull(affiliate_commission_from_child)) as affiliate_commission_from_child
from
(
select la.referral_marker
        , la.campaign_id
        , date_trunc('month',la.booked_at) as report_month
        , count(distinct la.internal_id) as referral_payouts
        , sum(zeroifnull(la.affiliate_commission)*la.rate_usd)/100 as affiliate_commission_from_child
        
from tp.lombard_actions la
        inner join ref_mod rm on rm.marker = la.referral_marker and rm.flag_child = 1
where date_trunc('month',la.booked_at) >= '2014-01-01'
        and la.action_state in ('paid')
        and la.type='referral'
        and la.action_type = 'booking'
        and la.id in
            (
            select distinct id
            from tp.lombard_actions
            where date_trunc('month',booked_at) >= '2014-01-01'
                    and type = 'action'
                    and action_type = 'booking'
                    and action_state in('paid')
            )
group by 1,2,3
) a
GROUP BY 1,2


-- Сколько аффов было и сколько приходило

with dates as
(
select date_trunc('month',pdate) as report_month
from sandbox.tp_all_dates
where pdate >= '2014-01-01' and pdate <= '2020-11-30'
group by 1
),

ref_mod as
(
select ai1.marker
        , ai2.marker as parent_marker
        , case when ai2.marker is NULL then 0 else 1 end as flag_child
        , date_trunc('month',to_date(ai1.created_at)) as child_reg_date
        , to_date(ai2.created_at) as parent_reg_date
from tp.users u
    inner join tp.affiliate_infos ai1 on ai1.affiliate_id = u.id and ai1.internal = 0
    left join tp.affiliate_infos ai2 on ai2.affiliate_id = u.parent_id and ai2.internal = 0
),

affs_count as
(
select d.report_month
        , sum(case when flag_child = 0 and d.report_month = a.child_reg_date then zeroifnull(markers) else 0 end) as new_non_child
        , sum(case when flag_child = 1 and d.report_month = a.child_reg_date then zeroifnull(markers) else 0 end) as new_child
        , sum(case when flag_child = 0 and d.report_month > a.child_reg_date then zeroifnull(markers) else 0 end) as old_non_child
        , sum(case when flag_child = 1 and d.report_month > a.child_reg_date then zeroifnull(markers) else 0 end) as old_child
from dates d
        left join
        (
        select child_reg_date
                , flag_child
                , count(distinct marker) as markers
        from ref_mod
        group by 1,2
        ) a 
        on d.report_month >= a.child_reg_date
group by 1
),


affs_active_network as 
(
select report_month
        , count(distinct case when flag_child = 0 and report_month = child_reg_date then marker end) as new_non_child_active_network
        , count(distinct case when flag_child = 1 and report_month = child_reg_date then marker end) as new_child_active_network
        , count(distinct case when flag_child = 0 and report_month > child_reg_date then marker end) as old_non_child_active_network
        , count(distinct case when flag_child = 1 and report_month > child_reg_date then marker end) as old_child_active_network
from
(
select date_trunc('month',la.booked_at) as report_month
        , la.marker
        , rm.flag_child
        , rm.child_reg_date
        , count(distinct la.internal_id) as bookings
        , sum(zeroifnull(la.action_profit)*la.rate_usd)/100 as action_profit
        , sum(zeroifnull(la.affiliate_commission)*la.rate_usd)/100 as affiliate_commission
from tp.lombard_actions la
        inner join ref_mod rm on rm.marker = la.marker
where date_trunc('month',la.booked_at) >= '2014-01-01'
        and la.action_state in ('paid')
        and la.type='action'
        and la.action_type = 'booking'
        and la.campaign_id not in(0,100,101)
group by 1,2,3,4
) a
group by 1
),

affs_active_salo_look as 
(
select report_month
        , count(distinct case when flag_child = 0 and report_month = child_reg_date then marker end) as new_non_child_active_salo_look
        , count(distinct case when flag_child = 1 and report_month = child_reg_date then marker end) as new_child_active_salo_look
        , count(distinct case when flag_child = 0 and report_month > child_reg_date then marker end) as old_non_child_active_salo_look
        , count(distinct case when flag_child = 1 and report_month > child_reg_date then marker end) as old_child_active_salo_look
from
(
select date_trunc('month',la.booked_at) as report_month
        , la.marker
        , rm.flag_child
        , rm.child_reg_date
        , count(distinct la.internal_id) as bookings
        , sum(zeroifnull(la.action_profit)*la.rate_usd)/100 as action_profit
        , sum(zeroifnull(la.affiliate_commission)*la.rate_usd)/100 as affiliate_commission
from tp.lombard_actions la
        inner join ref_mod rm on rm.marker = la.marker
where date_trunc('month',la.booked_at) >= '2014-01-01'
        and la.action_state in ('paid')
        and la.type='action'
        and la.action_type = 'booking'
        and la.campaign_id in(100,101)
group by 1,2,3,4
) a
group by 1
),

affs_active_any as 
(
select report_month
        , count(distinct case when flag_child = 0 and report_month = child_reg_date then marker end) as new_non_child_active_any
        , count(distinct case when flag_child = 1 and report_month = child_reg_date then marker end) as new_child_active_any
        , count(distinct case when flag_child = 0 and report_month > child_reg_date then marker end) as old_non_child_active_any
        , count(distinct case when flag_child = 1 and report_month > child_reg_date then marker end) as old_child_active_any
from
(
select date_trunc('month',la.booked_at) as report_month
        , la.marker
        , rm.flag_child
        , rm.child_reg_date
        , count(distinct la.internal_id) as bookings
        , sum(zeroifnull(la.action_profit)*la.rate_usd)/100 as action_profit
        , sum(zeroifnull(la.affiliate_commission)*la.rate_usd)/100 as affiliate_commission
from tp.lombard_actions la
        inner join ref_mod rm on rm.marker = la.marker
where date_trunc('month',la.booked_at) >= '2014-01-01'
        and la.action_state in ('paid')
        and la.type='action'
        and la.action_type = 'booking'
        and la.campaign_id not in(0)
group by 1,2,3,4
) a
group by 1
)

select ac.report_month
        , ac.new_child
        , ac.new_non_child
        , ac.old_child
        , ac.old_non_child
        , aan.new_child_active_network
        , aan.new_non_child_active_network
        , aan.old_child_active_network
        , aan.old_non_child_active_network
        , aasl.new_child_active_salo_look
        , aasl.new_non_child_active_salo_look
        , aasl.old_child_active_salo_look
        , aasl.old_non_child_active_salo_look
        , aaa.new_child_active_any
        , aaa.new_non_child_active_any
        , aaa.old_child_active_any
        , aaa.old_non_child_active_any
from affs_count ac
        left join affs_active_network aan on aan.report_month = ac.report_month
        left join affs_active_salo_look aasl on aasl.report_month = ac.report_month
        left join affs_active_any aaa on aaa.report_month = ac.report_month


-- LTV по историческим данным

with 

ref_mod as
(
select ai1.marker
        , ai2.marker as parent_marker
        , case when ai2.marker is NULL then 0 else 1 end as flag_child
        , date_trunc('month',to_date(ai1.created_at)) as child_reg_month
        , to_date(ai2.created_at) as parent_reg_date
from tp.users u
    inner join tp.affiliate_infos ai1 on ai1.affiliate_id = u.id and ai1.internal = 0
    left join tp.affiliate_infos ai2 on ai2.affiliate_id = u.parent_id and ai2.internal = 0
where to_date(ai1.created_at) >= '2014-01-01'
),

tp_affs_all_dates AS
(
select t1.report_date
        , t2.child_reg_month
        , t2.flag_child
        , months_between(t1.report_date,t2.child_reg_month)+1 as number_of_months
        , t2.marker
from 
    (
    select date_trunc('month',pdate) as report_date
    from sandbox.tp_all_dates
    where date_trunc('month',pdate) BETWEEN '2014-01-01' and '2020-11-01'
    ) t1
        
    LEFT JOIN
    (
    select marker
            , child_reg_month
            , flag_child
    from ref_mod
    ) t2
    on t1.report_date >= t2.child_reg_month

group by 1,2,3,4,5
),

tp_bookings AS
(
SELECT marker
        , date_trunc('month', booked_at) AS bookings_month
        , count(distinct internal_id) as bookings
        , sum(zeroifnull(action_profit) * rate_usd)/100 as profit
FROM tp.lombard_actions
WHERE to_date(booked_at) >= '2014-01-01'
        and type = 'action'
        and action_type = 'booking'
        and action_state in('paid','processing')
GROUP BY 1,2
)

select d.child_reg_month
        , d.number_of_months
        , d.flag_child
        , count(distinct d.marker) as markers
        , count(distinct case when zeroifnull(b.bookings) > 0 then b.marker end) as active_markers
        , sum(zeroifnull(b.bookings)) as bookings
        , sum(zeroifnull(b.profit)) as profit
from tp_affs_all_dates d
        left join tp_bookings as b on b.marker = d.marker and d.report_date = b.bookings_month
where d.number_of_months <= 100
group by 1,2,3


-- Тут расчет условных чисел

with 

ref_mod as
(
select ai1.marker
        , ai2.marker as parent_marker
        , case when ai2.marker is NULL then 0 else 1 end as flag_child
        , date_trunc('month',to_date(ai1.created_at)) as child_reg_month
        , to_date(ai2.created_at) as parent_reg_date
from tp.users u
    inner join tp.affiliate_infos ai1 on ai1.affiliate_id = u.id and ai1.internal = 0
    left join tp.affiliate_infos ai2 on ai2.affiliate_id = u.parent_id and ai2.internal = 0
where to_date(ai1.created_at) >= '2014-01-01'
        and to_date(ai1.created_at) is not null
),

tp_bookings AS
(
SELECT la.marker
        , min(date_trunc('month', booked_at)) AS bookings_month_min
        , max(date_trunc('month', booked_at)) AS bookings_month_max
        , count(distinct internal_id) as bookings
        , sum(zeroifnull(la.action_profit) * rate_usd)/100 as revenue
        , sum(zeroifnull(affiliate_commission) * rate_usd)/100 as profit
        , sum(zeroifnull(la.action_profit) * rate_usd)/100 - sum(zeroifnull(affiliate_commission) * rate_usd)/100 as free_cash
FROM tp.lombard_actions la
        inner join ref_mod rm on rm.marker = la.marker
WHERE to_date(booked_at) >= '2014-01-01'
        and type = 'action'
        and action_type = 'booking'
        and action_state in('paid')
GROUP BY 1
),

tp_payouts_ref AS
(
SELECT referral_marker as marker
        , min(date_trunc('month', booked_at)) AS payouts_month_min
        , max(date_trunc('month', booked_at)) AS payouts_month_max
        , count(distinct internal_id) as payouts_ref
        , sum(zeroifnull(affiliate_commission) * rate_usd)/100 as payouts_profit
FROM tp.lombard_actions la
        inner join ref_mod rm on rm.marker = la.referral_marker and rm.flag_child = 1
WHERE to_date(booked_at) >= '2014-01-01'
        and type = 'referral'
        and action_type = 'booking'
        and action_state in('paid')
GROUP BY 1
)


select flag_child
        , avg(case when bookings_month_max is not null then months_between(bookings_month_max,child_reg_month) else 0 end) as avgLifetime
        , appx_median(case when bookings_month_max is not null then months_between(bookings_month_max,child_reg_month) else 0 end) as medLifetime
        , sum(zeroifnull(bookings)) bookings
        , sum(zeroifnull(profit)) profit
        , sum(zeroifnull(payouts_ref)) payouts_ref
        , sum(zeroifnull(payouts_profit)) payouts_profit
        , sum(zeroifnull(free_cash)) free_cash
        , sum(zeroifnull(revenue)) revenue
        , count(distinct marker) as markers
        , sum(zeroifnull(free_cash))/count(distinct marker) as free_cash_per_aff
        , count(distinct case when zeroifnull(profit) > 100 then marker end) as markers_over100usd_com
        , count(distinct case when zeroifnull(payouts_profit) > 50 then marker end) as markers_over50usd_ref_com
        , count(distinct case when zeroifnull(bookings) >= 1 then marker end) as markers_over1book
        , count(distinct case when zeroifnull(bookings) >= 5 then marker end) as markers_over5book
        
        
from
(
select rm.marker
        , rm.child_reg_month
        , rm.flag_child
        , b.bookings_month_min
        , b.bookings_month_max
        , b.bookings
        , b.profit
        , b.revenue
        , b.free_cash
        , p.payouts_month_min
        , p.payouts_month_max
        , p.payouts_ref
        , p.payouts_profit
from ref_mod rm
        left join tp_bookings b on rm.marker = b.marker
        left join tp_payouts_ref p on rm.marker = p.marker
) a
group by 1


-- Сравнение по маркерам куда проебались букинги, что выплат реферальных больше

with 
ref_mod as
(
select ai1.marker
        , ai2.marker as parent_marker
        , case when ai2.marker is NULL then 0 else 1 end as flag_child
        , date_trunc('month',to_date(ai1.created_at)) as child_reg_month
        , to_date(ai2.created_at) as parent_reg_date
from tp.users u
    inner join tp.affiliate_infos ai1 on ai1.affiliate_id = u.id and ai1.internal = 0
    left join tp.affiliate_infos ai2 on ai2.affiliate_id = u.parent_id and ai2.internal = 0
where to_date(ai1.created_at) >= '2014-01-01'
        and to_date(ai1.created_at) is not null
)

SELECT coalesce(t1.marker,t2.marker) as marker
        , abs(zeroifnull(t1.bookings) - zeroifnull(t2.payouts_number)) as abs_diff
        , t1.bookings
        , t1.revenue
        , t1.comission
        , t1.profit
        , t2.payouts_number
        , t2.payouts_value
        , t2.parent_marker
FROM
(
SELECT la.marker
        , count(distinct internal_id) as bookings
        , sum(zeroifnull(la.action_profit) * rate_usd)/100 as revenue
        , sum(zeroifnull(affiliate_commission) * rate_usd)/100 as comission
        , sum(zeroifnull(la.action_profit) * rate_usd)/100 - sum(zeroifnull(affiliate_commission) * rate_usd)/100 as profit
FROM tp.lombard_actions la
        inner join ref_mod rm on rm.marker = la.marker
WHERE date_trunc('month',booked_at) = '2019-04-01'
        and type = 'action'
        and action_type = 'booking'
        and action_state in('paid')
        and la.campaign_id in(100,101)
GROUP BY 1
) t1

FULL JOIN
(
SELECT la.referral_marker as marker
        , max(la.marker) as parent_marker
        , count(distinct internal_id) as payouts_number
        , sum(zeroifnull(affiliate_commission) * rate_usd)/100 as payouts_value
FROM tp.lombard_actions la
        inner join ref_mod rm on rm.marker = la.referral_marker and rm.flag_child = 1
WHERE date_trunc('month',booked_at) = '2019-04-01'
        and type = 'referral'
        and action_type = 'booking'
        and action_state in('paid')
        and id in
            (
            select distinct id
            from tp.lombard_actions
            where date_trunc('month',booked_at) = '2019-04-01'
                    and type = 'action'
                    and action_type = 'booking'
                    and action_state in('paid')
            )
GROUP BY 1
) t2
on t1.marker = t2.marker

where parent_marker is not null
order by 2 desc
