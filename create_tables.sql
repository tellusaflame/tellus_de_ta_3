-- Creation of json_data table
CREATE TABLE IF NOT EXISTS json_data (
  userid INT NOT NULL,
  platform text NOT NULL,
  durationms INT,
  position INT,
  timestamp timestamp NOT NULL,
  _group INT,
  _user INT,
  post INT[],
  movie INT[],
  user_photo INT[],
  group_photo INT[]
);