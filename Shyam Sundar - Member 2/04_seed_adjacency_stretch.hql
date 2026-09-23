-- ============================================================
-- STRETCH — only needed once coordination/corridor work starts.
-- Seeds a simple 3-junction line (J1 - J2 - J3) so Member 3's
-- small-network demo and the coordination signal have adjacency
-- to work with.
-- ============================================================

INSERT INTO intersection_adjacency VALUES
  ('J1', 'J2', 'E', 400.0),
  ('J2', 'J1', 'W', 400.0),
  ('J2', 'J3', 'E', 350.0),
  ('J3', 'J2', 'W', 350.0);

-- Example ambulance route crossing J1 -> J2 -> J3, for the
-- corridor pre-clear demo. heading_deg ~ eastbound (90).
INSERT INTO ambulance_events VALUES
  ('AMB1', '2026-09-23 10:00:00', 'J1', 90.0, 'J2', 25.0),
  ('AMB1', '2026-09-23 10:00:25', 'J2', 90.0, 'J3', 20.0),
  ('AMB1', '2026-09-23 10:00:45', 'J3', 90.0, NULL, 0.0);
