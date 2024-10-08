SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';
---
SELECT k_word
FROM keyword
limit 10;
--
SELECT pk_word,
pk_post_id
FROM post_keyword
limit 10;
--
SELECT body
FROM post
where p_post_id = 394158;