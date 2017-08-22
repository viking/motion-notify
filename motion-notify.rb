require 'getoptlong'
require 'date'
require 'drb'

class Worker
  def initialize(config_path, folder_id, unlink)
    require 'google_drive'
    @config_path = config_path
    @folder_id = folder_id
    @unlink = unlink
    @mutex = Mutex.new
  end

  def upload(datetime, frame, picture_path)
    @mutex.synchronize do
      coll = parent

      # find or create folder for date
      date_title = datetime.strftime("%Y-%m-%d")
      sub_coll = coll.subcollection_by_title(date_title)
      if sub_coll.nil?
        sub_coll = coll.create_subcollection(date_title)
      end

      # upload picture
      sub_coll.upload_from_file(picture_path, "#{datetime.strftime("%H:%M:%S")}-#{frame}")
      if @unlink
        File.unlink(picture_path)
      end
    end
  end

  private

  def session
    @session ||= GoogleDrive::Session.from_config(@config_path)
  end

  def parent
    @parent ||= session.file_by_id(@folder_id)
  end
end

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

-D drb-uri, --drb drb-uri:
   URI of DRb process
EOF
end

opts = GetoptLong.new(
  [ '--help',       '-h',  GetoptLong::NO_ARGUMENT ],
  [ '--folder-id',  '-f',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--config',     '-c',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--datetime',   '-d',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--frame',      '-F',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--picture',    '-p',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--drb',        '-D',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--no-unlink',  '-U',  GetoptLong::NO_ARGUMENT ],
)

program_name = $0
folder_id = nil
config_path = nil
datetime = nil
frame = nil
picture_path = nil
unlink = true
drb_uri = "druby://localhost:8787"

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
  when '--drb'
    drb_uri = arg
  when '--no-unlink'
    unlink = false
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

# try to connect to existing DRb process first
server = DRbObject.new_with_uri(drb_uri)
tried = false
begin
  server.upload(datetime, frame, picture_path)
rescue DRb::DRbConnError => exp
  # failed, boot DRb
  fork do
    DRb.start_service(drb_uri, Worker.new(config_path, folder_id, unlink))
    DRb.thread.join
  end
  sleep 5
  if !tried
    tried = true
    retry
  else
    raise "Couldn't start server"
  end
end
