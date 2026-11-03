-- The wire form (prn_ + 22 base64url characters) of the principal id of the api workload row.
SELECT 'prn_' || translate(rtrim(encode(decode(replace(principal_id::text, '-', ''), 'hex'), 'base64'), '='), '+/', '-_')
FROM auth.workloads WHERE name = 'api';
