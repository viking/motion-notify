require 'getoptlong'
require 'date'
require 'google_drive'

def print_usage(program_name)
    puts <<EOF
#{program_name} [OPTION]

-h, --help:
   show help

-f folder-id, --folder-id folder-id:
   Google Drive parent folder identifier

-c config-path, --config config-path:
   Configuration file for authentication

-d datetime, --datetime datetime:
   Date and time of event (YYYY-mm-dd HH:MM:SS)

-F frame, --frame frame:
   Frame number

-p picture-path, --picture picture-path:
   Full path of picture
EOF
end

opts = GetoptLong.new(
  [ '--help',       '-h',  GetoptLong::NO_ARGUMENT ],
  [ '--folder-id',  '-f',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--config',     '-c',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--datetime',   '-d',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--frame',      '-F',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--picture',    '-p',  GetoptLong::REQUIRED_ARGUMENT ],
)

program_name = $0
folder_id = nil
config_path = nil
datetime = nil
frame = nil
picture_path = nil

opts.each do |opt, arg|
  case opt
  when '--help'
    print_usage(program_name)
    exit
  when '--folder-id'
    folder_id = arg
  when '--config'
    config_path = arg
  when '--datetime'
    datetime = arg
  when '--frame'
    frame = arg
  when '--picture'
    picture_path = arg
  end
end

valid = true
if folder_id.nil?
  puts "--folder-id is required"
  valid = false
end
if config_path.nil?
  puts "--config is required"
  valid = false
end
if datetime.nil?
  puts "--datetime is required"
  valid = false
else
  begin
    datetime = DateTime.strptime(datetime, "%Y-%m-%d %H:%M:%S")
  rescue ArgumentError
    puts "#{datetime} is not a valid datetime"
    valid = false
  end
end
if frame.nil?
  puts "--frame is required"
  valid = false
end
if picture_path.nil?
  puts "--picture is required"
  valid = false
end

if !valid
  print_usage(program_name)
  exit
end

session = GoogleDrive::Session.from_config(config_path)
coll = session.file_by_id(folder_id)
if !coll.is_a?(GoogleDrive::Collection)
  puts "--folder-id does not point to a folder"
  print_usage(program_name)
  exit
end

# find or create folder for date
date_title = datetime.strftime("%Y-%m-%d")
sub_coll = coll.subcollection_by_title(date_title)
if sub_coll.nil?
  sub_coll = coll.create_subcollection(date_title)
end

# upload picture
begin
  sub_coll.upload_from_file(picture_path, "#{datetime.strftime("%H:%M:%S")}-#{frame}")
  File.unlink(picture_path)
end
