-- Creation of json_data table
CREATE TABLE IF NOT EXISTS json_data (
  userid INT NOT NULL,
  platform text NOT NULL,
  durationms INT,
  feed_position INT,
  ts timestamp NOT NULL,
  owners_group INT[],
  owners_user INT[],
  post INT[],
  movie INT[],
  user_photo INT[],
  group_photo INT[]
);