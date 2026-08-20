-- Baseline stopword list for trending_keywords tokenization filtering.

INSERT INTO stopwords (word) VALUES
    ('the'), ('a'), ('an'), ('and'), ('or'), ('but'), ('if'), ('then'), ('so'),
    ('of'), ('to'), ('in'), ('on'), ('at'), ('by'), ('for'), ('with'), ('about'),
    ('as'), ('is'), ('are'), ('was'), ('were'), ('be'), ('been'), ('being'),
    ('this'), ('that'), ('these'), ('those'), ('it'), ('its'), ('i'), ('you'),
    ('he'), ('she'), ('we'), ('they'), ('them'), ('his'), ('her'), ('our'), ('your'),
    ('not'), ('no'), ('do'), ('does'), ('did'), ('has'), ('have'), ('had'),
    ('will'), ('would'), ('can'), ('could'), ('should'), ('what'), ('when'),
    ('where'), ('who'), ('why'), ('how'), ('all'), ('any'), ('from'), ('into'),
    ('up'), ('down'), ('out'), ('over'), ('under'), ('again'), ('new'), ('now'),
    ('hn'), ('show'), ('ask')
ON CONFLICT (word) DO NOTHING;
