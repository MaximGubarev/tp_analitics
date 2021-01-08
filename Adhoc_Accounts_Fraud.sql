-- Эдхок по расследованию фрода
select t1.order_id
        , t1.marker
        , t1.booking_dttm
        , t1.processing_dttm
        , t2.paid_dttm
        , t3.cancelled_dttm
        , datediff(t2.paid_dttm,t1.processing_dttm) as days_between_processing_paid
        , datediff(t3.cancelled_dttm,t1.processing_dttm) as days_between_processing_cancelled
        , datediff(t3.cancelled_dttm,t2.paid_dttm) as days_between_paid_cancelled
from 
(

) t1

LEFT JOIN
(
select id as order_id
        , min(updated_at) as paid_dttm
from tp.lombard_actions_log
where to_date(booked_at) >= '2020-01-01'
        and campaign_id = 99
        and marker in(296854, 272750, 272099)
        and type = 'action'
        and action_type = 'booking'
        and action_state = 'paid'
group by 1
) t2
on t1.order_id = t2.order_id

LEFT JOIN
(
select id as order_id
        , min(updated_at) as cancelled_dttm
from tp.lombard_actions_log
where to_date(booked_at) >= '2020-01-01'
        and campaign_id = 99
        and marker in(296854, 272750, 272099)
        and type = 'action'
        and action_type = 'booking'
        and action_state = 'cancelled'
group by 1
) t3
on t1.order_id = t3.order_id