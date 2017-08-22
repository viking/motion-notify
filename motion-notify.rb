require 'getoptlong'
require 'date'
require 'fileutils'
require 'drb'

class Worker
  def initialize(config_path, folder_id, unlink, log_path)
    @config_path = config_path
    @folder_id = folder_id
    @unlink = unlink
    @mutex = Mutex.new
    @queue = []
    if log_path
      require 'logger'
      @logger = Logger.new(log_path)
    end
    start_poll
  end

  def queue(datetime, frame, picture_path)
    @mutex.synchronize do
      @queue.push([datetime, frame, picture_path])
      log(:info, "Queued: #{datetime.to_s} (#{frame}), #{picture_path}")
    end
  end

  def start_poll
    @poll ||= Thread.new do
      loop do
        @mutex.synchronize do
          if !@queue.empty?
            datetime, frame, picture_path = @queue.shift
            log(:info, "Processing: #{datetime.to_s} (#{frame}), #{picture_path}")
            begin
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
              log(:info, "Finished: #{datetime.to_s} (#{frame}), #{picture_path}")
            rescue Exception => exp
              log(:fatal, exp)
            end
          end
        end
        sleep 1
      end
    end
  end

  private

  def session
    if @session.nil?
      require 'google_drive'
      @session = GoogleDrive::Session.from_config(@config_path)
    end
    @session
  end

  def parent
    @parent ||= session.file_by_id(@folder_id)
  end

  def log(severity, msg)
    if @logger
      severity =
        case severity
        when :info  then Logger::INFO
        when :fatal then Logger::FATAL
        end
      @logger.log(severity, msg)
    end
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

-U, --no-unlink:
   Disable unlinking of files

-s, --start:
   Start background job

-P pid-path, --pid pid-path:
   Path of PID file

-l log-path, --log log-path:
   Path of log file
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
  [ '--start',      '-s',  GetoptLong::NO_ARGUMENT ],
  [ '--pid',        '-P',  GetoptLong::REQUIRED_ARGUMENT ],
  [ '--log',        '-l',  GetoptLong::REQUIRED_ARGUMENT ],
)

program_name = $0
folder_id = nil
config_path = nil
datetime = nil
frame = nil
picture_path = nil
unlink = true
drb_uri = "druby://localhost:8787"
start_background = false
pid_path = nil
log_path = nil

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
  when '--start'
    start_background = true
  when '--pid'
    pid_path = arg
  when '--log'
    log_path = arg
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
if pid_path.nil?
  puts "--pid is required"
  valid = false
end
if !start_background
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
end

if !valid
  print_usage(program_name)
  exit
end

if start_background
  at_exit { File.unlink(pid_path) }
  DRb.start_service(drb_uri, Worker.new(config_path, folder_id, unlink, log_path))
  DRb.thread.join
else
  # try to connect to existing DRb process first
  server = DRbObject.new_with_uri(drb_uri)
  tried = 0
  begin
    server.queue(datetime, frame, picture_path)
  rescue DRb::DRbConnError => exp
    if tried >= 5
      raise "Tried 5 times to connect to background process, but failed"
    end

    # failed, boot DRb if pid file doesn't exist
    if !File.exist?(pid_path)
      FileUtils.touch(pid_path)
      args = [
        program_name, "--start",
        "--config", config_path,
        "--folder-id", folder_id,
        "--pid", pid_path
      ]
      if log_path
        args << "--log"
        args << log_path
      end
      if !unlink
        args << "--no-unlink"
      end
      pid = spawn(RbConfig.ruby, *args)
      File.open(pid_path, 'w') { |f| f.puts(pid) }
    end

    sleep 1
    tried += 1
    retry
  end
end
