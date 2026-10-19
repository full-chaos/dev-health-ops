-- Add event_ts and day to work_graph_edges for time-window filtering
ALTER TABLE work_graph_edges ADD COLUMN IF NOT EXISTS event_ts DateTime64(3, 'UTC') DEFAULT now();
ALTER TABLE work_graph_edges ADD COLUMN IF NOT EXISTS day Date DEFAULT toDate(event_ts);
