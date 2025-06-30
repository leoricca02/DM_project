-- =====================================================
-- QUERY OLAP PER ANALISI E-COMMERCE
-- Progetto: Data Warehouse E-commerce Brasiliano
-- =====================================================

-- =====================================================
-- 1. ROLL-UP: Da mese a trimestre a anno
-- Analisi vendite con aggregazione temporale crescente
-- =====================================================
-- Livello più dettagliato: mensile
SELECT 
    dt.year,
    dt.quarter,
    dt.month_name,
    COUNT(*) as numero_ordini,
    SUM(fs.price) as totale_vendite,
    AVG(fs.price) as prezzo_medio
FROM fact_sales fs
JOIN dim_time dt ON fs.time_key = dt.time_key
WHERE dt.year = 2017
GROUP BY dt.year, dt.quarter, dt.month_name, dt.month_num
ORDER BY dt.year, dt.month_num;

-- Roll-up a trimestre
SELECT 
    dt.year,
    dt.quarter,
    COUNT(*) as numero_ordini,
    SUM(fs.price) as totale_vendite,
    AVG(fs.price) as prezzo_medio
FROM fact_sales fs
JOIN dim_time dt ON fs.time_key = dt.time_key
WHERE dt.year = 2017
GROUP BY dt.year, dt.quarter
ORDER BY dt.year, dt.quarter;

-- Roll-up ad anno
SELECT 
    dt.year,
    COUNT(*) as numero_ordini,
    SUM(fs.price) as totale_vendite,
    AVG(fs.price) as prezzo_medio
FROM fact_sales fs
JOIN dim_time dt ON fs.time_key = dt.time_key
GROUP BY dt.year
ORDER BY dt.year;

-- =====================================================
-- 2. DRILL-DOWN: Da stato a città
-- Analisi geografica dal generale al particolare
-- =====================================================
-- Livello stato
SELECT 
    dc.customer_state as stato,
    COUNT(*) as numero_ordini,
    SUM(fs.price) as totale_vendite,
    COUNT(DISTINCT fs.customer_key) as numero_clienti
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
WHERE dc.customer_state IN ('SP', 'RJ', 'MG')  -- Stati principali
GROUP BY dc.customer_state
ORDER BY totale_vendite DESC;

-- Drill-down a città (per stato SP)
SELECT 
    dc.customer_state as stato,
    dc.customer_city as citta,
    COUNT(*) as numero_ordini,
    SUM(fs.price) as totale_vendite,
    COUNT(DISTINCT fs.customer_key) as numero_clienti
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_key = dc.customer_key
WHERE dc.customer_state = 'SP'  -- Focus su San Paolo
GROUP BY dc.customer_state, dc.customer_city
ORDER BY totale_vendite DESC
LIMIT 10;

-- =====================================================
-- 3. SLICE: Analisi per periodo specifico
-- Vista su un "slice" temporale dei dati
-- =====================================================
-- Slice: solo estate 2017 (dic-gen-feb in Brasile)
SELECT 
    dp.product_category_name_english as categoria,
    dt.month_name as mese,
    COUNT(*) as vendite,
    SUM(fs.price) as ricavo_totale,
    AVG(fs.review_score) as rating_medio
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_time dt ON fs.time_key = dt.time_key
WHERE dt.year = 2017 
    AND dt.season_brazil = 'Verão'  -- Estate brasiliana
    AND dp.product_category_name_english IS NOT NULL
GROUP BY dp.product_category_name_english, dt.month_name, dt.month_num
ORDER BY ricavo_totale DESC;

-- =====================================================
-- 4. DICE: Analisi multidimensionale
-- Combinazione di più dimensioni con filtri
-- =====================================================
-- Dice: categoria prodotto + metodo pagamento + periodo
SELECT 
    dp.product_category_name_english as categoria,
    dpm.payment_type as tipo_pagamento,
    dt.quarter as trimestre,
    dt.year as anno,
    COUNT(*) as numero_vendite,
    SUM(fs.price) as totale,
    ROUND(AVG(fs.price)::numeric, 2) as prezzo_medio
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_payment dpm ON fs.payment_key = dpm.payment_key
JOIN dim_time dt ON fs.time_key = dt.time_key
WHERE dp.product_category_name_english IN (
    SELECT product_category_name_english 
    FROM dim_product 
    WHERE product_category_name_english IS NOT NULL
    GROUP BY product_category_name_english
    ORDER BY COUNT(*) DESC
    LIMIT 3  -- Top 3 categorie
)
AND dpm.payment_type IN ('credit_card', 'boleto')
AND dt.year IN (2017, 2018)
GROUP BY dp.product_category_name_english, dpm.payment_type, dt.quarter, dt.year
ORDER BY categoria, anno, trimestre;

-- =====================================================
-- 5. PIVOT: Matrice categorie vs mesi
-- Trasformazione righe in colonne per analisi comparativa
-- =====================================================
SELECT 
    dp.product_category_name_english as categoria,
    SUM(CASE WHEN dt.month_num = 1 THEN fs.price ELSE 0 END) as gen,
    SUM(CASE WHEN dt.month_num = 2 THEN fs.price ELSE 0 END) as feb,
    SUM(CASE WHEN dt.month_num = 3 THEN fs.price ELSE 0 END) as mar,
    SUM(CASE WHEN dt.month_num = 4 THEN fs.price ELSE 0 END) as apr,
    SUM(CASE WHEN dt.month_num = 5 THEN fs.price ELSE 0 END) as mag,
    SUM(CASE WHEN dt.month_num = 6 THEN fs.price ELSE 0 END) as giu,
    SUM(fs.price) as totale_semestre
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_time dt ON fs.time_key = dt.time_key
WHERE dt.year = 2017 
    AND dt.month_num <= 6
    AND dp.product_category_name_english IS NOT NULL
GROUP BY dp.product_category_name_english
ORDER BY totale_semestre DESC
LIMIT 10;

-- =====================================================
-- 6. RANKING: Top venditori per stato
-- Utilizzo di window functions per classifiche
-- =====================================================
WITH vendite_venditori AS (
    SELECT 
        ds.seller_state as stato,
        ds.seller_id as venditore,
        COUNT(*) as numero_vendite,
        SUM(fs.price) as totale_vendite,
        AVG(fs.review_score) as rating_medio
    FROM fact_sales fs
    JOIN dim_seller ds ON fs.seller_key = ds.seller_key
    GROUP BY ds.seller_state, ds.seller_id
)
SELECT 
    stato,
    venditore,
    numero_vendite,
    totale_vendite,
    rating_medio,
    RANK() OVER (PARTITION BY stato ORDER BY totale_vendite DESC) as rank_stato
FROM vendite_venditori
WHERE stato IN ('SP', 'RJ', 'MG')
ORDER BY stato, rank_stato
LIMIT 15;

-- =====================================================
-- 7. ANALISI CROSS-STATE: Vendite locali vs remote
-- =====================================================
SELECT 
    CASE WHEN is_cross_state_sale THEN 'Interstate' ELSE 'Local' END as tipo_vendita,
    COUNT(*) as numero_ordini,
    ROUND(AVG(freight_value)::numeric, 2) as costo_spedizione_medio,
    ROUND(AVG(freight_percentage)::numeric, 2) as percentuale_spedizione,
    ROUND(SUM(price)::numeric, 2) as ricavo_totale
FROM fact_sales
GROUP BY is_cross_state_sale
ORDER BY numero_ordini DESC;

-- =====================================================
-- 8. TREND ANALYSIS: Crescita mensile
-- Analisi delle tendenze temporali
-- =====================================================
WITH vendite_mensili AS (
    SELECT 
        dt.year,
        dt.month_num,
        dt.month_name,
        COUNT(*) as ordini,
        SUM(fs.price) as vendite_totali
    FROM fact_sales fs
    JOIN dim_time dt ON fs.time_key = dt.time_key
    GROUP BY dt.year, dt.month_num, dt.month_name
)
SELECT 
    year,
    month_name,
    ordini,
    vendite_totali,
    LAG(vendite_totali, 1) OVER (ORDER BY year, month_num) as vendite_mese_prec,
    ROUND((vendite_totali - LAG(vendite_totali, 1) OVER (ORDER BY year, month_num)) / 
          NULLIF(LAG(vendite_totali, 1) OVER (ORDER BY year, month_num), 0) * 100, 2) as crescita_percentuale
FROM vendite_mensili
ORDER BY year, month_num;

-- =====================================================
-- 9. BASKET ANALYSIS: Prodotti per ordine
-- Analisi semplice del carrello
-- =====================================================
WITH ordini_multiprodotto AS (
    SELECT 
        fs.order_id,
        COUNT(DISTINCT fs.product_key) as numero_prodotti,
        SUM(fs.price) as valore_ordine,
        AVG(fs.price) as prezzo_medio_prodotto
    FROM fact_sales fs
    GROUP BY fs.order_id
)
SELECT 
    CASE 
        WHEN numero_prodotti = 1 THEN '1 prodotto'
        WHEN numero_prodotti BETWEEN 2 AND 3 THEN '2-3 prodotti'
        WHEN numero_prodotti BETWEEN 4 AND 5 THEN '4-5 prodotti'
        ELSE '6+ prodotti'
    END as prodotti_per_ordine,
    COUNT(*) as numero_ordini,
    AVG(valore_ordine) as valore_medio_ordine,
    AVG(prezzo_medio_prodotto) as prezzo_medio
FROM ordini_multiprodotto
GROUP BY 
    CASE 
        WHEN numero_prodotti = 1 THEN '1 prodotto'
        WHEN numero_prodotti BETWEEN 2 AND 3 THEN '2-3 prodotti'
        WHEN numero_prodotti BETWEEN 4 AND 5 THEN '4-5 prodotti'
        ELSE '6+ prodotti'
    END
ORDER BY numero_ordini DESC;

-- =====================================================
-- 10. PERFORMANCE COMPARISON: Weekend vs Weekday
-- Confronto vendite giorni feriali vs weekend
-- =====================================================
SELECT 
    CASE 
        WHEN dt.is_weekend THEN 'Weekend'
        ELSE 'Giorni Feriali'
    END as tipo_giorno,
    COUNT(*) as numero_ordini,
    SUM(fs.price) as vendite_totali,
    AVG(fs.price) as ticket_medio,
    AVG(fs.review_score) as soddisfazione_media,
    COUNT(DISTINCT fs.customer_key) as clienti_unici
FROM fact_sales fs
JOIN dim_time dt ON fs.time_key = dt.time_key
GROUP BY dt.is_weekend
ORDER BY vendite_totali DESC;