SELECT
  count(*)
FROM
  (
    select
      PIP_ID,
      NSC_INVOICE_DATE
    FROM
      (
        select
          DIST_COMP.PIP_ID PIP_ID,
          DIST_COMP.NSC_INVOICE_DATE NSC_INVOICE_DATE
        from
          (
            SELECT
              DISTINCT STG_EMOT_BTNIHDR.PIP_ID AS PIP_ID,
              first_value(STG_EMOT_BTNIHDR.INV_DATE) over (
                partition by STG_EMOT_BTNIHDR.PIP_ID
                order by
                  STG_EMOT_BTNIHDR.ORD_DATE -- rows between unbounded preceding
                  -- and unbounded following
              ) AS NSC_INVOICE_DATE
            FROM
              STG.STG_EMOT_BTNIHDR STG_EMOT_BTNIHDR
            WHERE
              STG_EMOT_BTNIHDR.INV_ORIG = 'NSC'
          ) DIST_COMP
      )
  ) COUNT_TABLE
