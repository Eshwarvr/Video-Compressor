from flask import Flask, render_template, request, jsonify, send_file
import os
import subprocess
import json
import uuid
import zipfile
import shutil
import threading
import time
import multiprocessing
from datetime import datetime
from werkzeug.utils import secure_filename
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import traceback

app = Flask(__name__)

# =============== CONFIGURATION ===============
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'compressed'
ALLOWED_EXTENSIONS = {'mp4', 'mov', 'avi', 'mkv', 'wmv', 'flv', 'webm', 'mpeg', 'mpg', '3gp'}
MAX_FILE_SIZE = 10 * 1024 * 1024 * 1024  # 10GB

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['OUTPUT_FOLDER'] = OUTPUT_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# =============== MULTI-THREADING CONFIGURATION ===============
CPU_CORES = multiprocessing.cpu_count()
MAX_CONCURRENT_JOBS = max(5, int(CPU_CORES * 1.5))

print(f"\n🖥️  System Configuration:")
print(f"   CPU Cores: {CPU_CORES}")
print(f"   Max Concurrent Compressions: {MAX_CONCURRENT_JOBS}")

# Thread-safe data structures
job_queue = Queue()
compression_jobs = {}
jobs_lock = threading.Lock()
active_threads = set()
active_threads_lock = threading.Lock()
queue_processor_running = False

# Thread pool executor
executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_JOBS, thread_name_prefix='compression_')

# =============== LOGGING ===============
def log_debug(job_id, message):
    """Thread-safe debug logging"""
    timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    queue_size = job_queue.qsize()
    active_count = len(active_threads)
    print(f"[{timestamp}] [Q:{queue_size:2d}] [A:{active_count:2d}/{MAX_CONCURRENT_JOBS}] [{job_id[:8]}] {message}", flush=True)

def update_job_status(job_id, **kwargs):
    """Thread-safe job status update"""
    with jobs_lock:
        if job_id in compression_jobs:
            compression_jobs[job_id].update(kwargs)

# =============== FILE VALIDATION ===============
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def allowed_zip(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'zip'

# =============== ZIP HANDLING ===============
def extract_zip_videos(zip_path, extract_folder):
    """Extract videos from ZIP file"""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        video_files = []
        for root, dirs, files in os.walk(extract_folder):
            for file in files:
                if allowed_file(file):
                    video_files.append(os.path.join(root, file))
        return sorted(video_files), None
    except Exception as e:
        return None, f"ZIP extraction error: {str(e)}"

# =============== VIDEO INFO ===============
def get_video_info(video_path):
    """Get video metadata using FFprobe"""
    try:
        cmd = [
            'ffprobe', '-v', 'error',
            '-select_streams', 'v:0',
            '-show_entries', 'format=duration:stream=width,height,codec_name,r_frame_rate',
            '-of', 'json',
            video_path
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            format_info = data.get('format', {})
            streams = data.get('streams', [])
            duration = float(format_info.get('duration', 0))
            info = {
                'duration': duration,
                'size': os.path.getsize(video_path),
                'bit_rate': format_info.get('bit_rate', 'unknown'),
            }
            if streams:
                stream = streams[0]
                info.update({
                    'width': stream.get('width', 0),
                    'height': stream.get('height', 0),
                    'codec': stream.get('codec_name', 'unknown'),
                })
            return info
        else:
            log_debug('ffprobe', f"Error: {result.stderr}")
            return None
    except Exception as e:
        log_debug('ffprobe', f"Error getting video info: {e}")
        return None

# =============== COMPRESSION ENGINE ===============
def compress_video(job_id, input_path, output_path, settings):
    """
    Compress video using FFmpeg with multi-threading optimizations.
    Runs in thread pool executor.
    """
    log_debug(job_id, "🚀 COMPRESSION START")
    try:
        with jobs_lock:
            if job_id not in compression_jobs:
                log_debug(job_id, "❌ Job not found")
                return

        # Get video information
        video_info = get_video_info(input_path)
        if not video_info:
            update_job_status(job_id, status='failed', message='Could not read video file')
            log_debug(job_id, "❌ Failed to get video info")
            return

        total_duration = video_info.get('duration', 0)
        log_debug(job_id, f"📹 Video duration: {total_duration:.1f}s")

        if total_duration <= 0:
            update_job_status(job_id, status='failed', message='Invalid video duration')
            return

        # Build FFmpeg command
        cmd = ['ffmpeg', '-i', input_path, '-y']

        # Video codec with threading
        codec = settings.get('codec', 'h264')
        if codec == 'h265':
            cmd.extend(['-c:v', 'libx265'])
            cmd.extend(['-x265-params', f'threads={CPU_CORES}'])
            log_debug(job_id, f"🎬 H.265 (threads={CPU_CORES})")
        else:
            cmd.extend(['-c:v', 'libx264'])
            cmd.extend(['-threads', str(CPU_CORES)])
            log_debug(job_id, f"🎬 H.264 (threads={CPU_CORES})")

        # Quality
        preset = settings.get('preset', 'balanced')
        crf_map = {'high': 18, 'balanced': 23, 'compressed': 28, 'custom': int(settings.get('crf', 23))}
        crf = crf_map.get(preset, 23)
        cmd.extend(['-crf', str(crf)])

        # Speed preset
        speed = settings.get('speed', 'fast')
        cmd.extend(['-preset', speed])

        # Resolution
        resolution = settings.get('resolution', 'original')
        if resolution != 'original':
            cmd.extend(['-vf', f'scale={resolution}'])

        # Audio
        if settings.get('preserve_audio', True):
            cmd.extend(['-c:a', 'aac', '-b:a', '128k'])
        else:
            cmd.extend(['-c:a', 'copy'])

        # Progress tracking
        cmd.extend(['-progress', 'pipe:1', '-v', 'quiet', output_path])

        # Update status
        update_job_status(job_id,
            status='processing',
            progress=0,
            message='Encoding...',
            output_path=output_path,
            processing_started=datetime.now().isoformat()
        )

        # Start FFmpeg
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1
            )
        except Exception as e:
            log_debug(job_id, f"❌ Failed to start FFmpeg: {e}")
            update_job_status(job_id, status='failed', message=f'FFmpeg start error: {e}')
            return

        log_debug(job_id, "✅ FFmpeg started")

        # Read progress
        try:
            for line in process.stdout:
                if not line:
                    continue
                line = line.strip()
                if 'out_time_ms=' in line:
                    try:
                        time_ms = int(line.split('=')[1])
                        current_time = time_ms / 1000000
                        if total_duration > 0:
                            progress = min(int((current_time / total_duration) * 100), 99)
                            update_job_status(job_id, progress=progress, message=f'Encoding... {progress}%')
                    except (ValueError, IndexError):
                        pass
        except Exception as e:
            log_debug(job_id, f"⚠️  Error reading progress: {e}")

        # Wait for completion
        process.wait()

        if process.returncode == 0:
            log_debug(job_id, "✅ Compression completed")
            input_size = os.path.getsize(input_path)
            output_size = os.path.getsize(output_path)
            compression_ratio = (1 - (output_size / input_size)) * 100 if input_size > 0 else 0

            update_job_status(job_id,
                status='completed',
                progress=100,
                message='✅ Compression completed',
                input_size=input_size,
                output_size=output_size,
                compression_ratio=round(compression_ratio, 2),
                completed_at=datetime.now().isoformat()
            )
        else:
            log_debug(job_id, f"❌ FFmpeg error (code: {process.returncode})")
            update_job_status(job_id, status='failed', message='Compression failed')

    except Exception as e:
        log_debug(job_id, f"❌ Exception: {e}")
        update_job_status(job_id, status='failed', message=f'Error: {str(e)}')
        traceback.print_exc()

    finally:
        with active_threads_lock:
            active_threads.discard(job_id)
        log_debug(job_id, "🏁 COMPRESSION END")

# =============== BACKGROUND QUEUE PROCESSOR ===============
def process_queue_background():
    """Background thread that continuously processes the queue"""
    log_debug('QUEUE', "🔄 Queue processor started")

    while queue_processor_running:
        try:
            if not job_queue.empty() and len(active_threads) < MAX_CONCURRENT_JOBS:
                try:
                    priority, job_id = job_queue.get_nowait()

                    with jobs_lock:
                        if job_id not in compression_jobs:
                            log_debug('QUEUE', f"⚠️  Job {job_id[:8]} not found")
                            continue

                        job = compression_jobs[job_id]
                        if job['status'] not in ['queued', 'uploaded']:
                            continue

                    with active_threads_lock:
                        active_threads.add(job_id)

                    log_debug(job_id, f"📤 Starting (Active: {len(active_threads)}/{MAX_CONCURRENT_JOBS})")

                    # Submit compression tasks
                    with jobs_lock:
                        if compression_jobs[job_id]['file_type'] == 'single':
                            executor.submit(compress_video,
                                job_id,
                                compression_jobs[job_id]['input_path'],
                                compression_jobs[job_id].get('output_path',
                                    os.path.join(OUTPUT_FOLDER, f"compressed_{job_id}.mp4")),
                                compression_jobs[job_id].get('settings', {})
                            )

                        elif compression_jobs[job_id]['file_type'] == 'zip':
                            # For ZIP: each video in separate job
                            for video_file in compression_jobs[job_id]['video_files']:
                                name_only = os.path.splitext(os.path.basename(video_file))[0]
                                output_filename = f"{name_only}_compressed.mp4"
                                output_path = os.path.join(OUTPUT_FOLDER, output_filename)

                                executor.submit(compress_video,
                                    job_id,
                                    video_file,
                                    output_path,
                                    compression_jobs[job_id].get('settings', {})
                                )

                except Exception as e:
                    log_debug('QUEUE', f"Error: {e}")
                    time.sleep(0.1)
            else:
                time.sleep(0.1)

        except Exception as e:
            log_debug('QUEUE', f"Background processor error: {e}")
            time.sleep(0.1)

    log_debug('QUEUE', "🛑 Queue processor stopped")

# Start background queue processor
queue_processor_thread = None

def start_queue_processor():
    """Start the background queue processor thread"""
    global queue_processor_running, queue_processor_thread
    if not queue_processor_running:
        queue_processor_running = True
        queue_processor_thread = threading.Thread(target=process_queue_background, daemon=True)
        queue_processor_thread.start()
        log_debug('QUEUE', "✅ Queue processor thread started")

# =============== ROUTES ===============

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    job_id = str(uuid.uuid4())
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    try:
        if allowed_file(file.filename):
            # Single video file
            unique_filename = f"{timestamp}_{secure_filename(file.filename)}"
            input_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
            file.save(input_path)

            video_info = get_video_info(input_path)
            output_filename = f"compressed_{job_id}.mp4"

            with jobs_lock:
                compression_jobs[job_id] = {
                    'job_id': job_id,
                    'status': 'uploaded',
                    'progress': 0,
                    'message': '⏳ Ready for compression',
                    'input_path': input_path,
                    'input_filename': file.filename,
                    'output_filename': output_filename,
                    'output_path': os.path.join(OUTPUT_FOLDER, output_filename),
                    'file_type': 'single',
                    'video_info': video_info,
                    'created_at': datetime.now().isoformat(),
                    'settings': {}
                }

            return jsonify({
                'job_id': job_id,
                'filename': file.filename,
                'file_type': 'single',
                'video_info': video_info
            }), 200

        elif allowed_zip(file.filename):
            # ZIP file with multiple videos
            unique_zip_name = f"{timestamp}_{secure_filename(file.filename)}"
            zip_path = os.path.join(app.config['UPLOAD_FOLDER'], unique_zip_name)
            extract_folder = os.path.join(app.config['UPLOAD_FOLDER'], f'zip_{job_id}')

            file.save(zip_path)
            os.makedirs(extract_folder, exist_ok=True)

            video_files, error = extract_zip_videos(zip_path, extract_folder)
            if error or not video_files:
                return jsonify({'error': 'No valid videos in ZIP'}), 400

            first_video_info = get_video_info(video_files[0])

            with jobs_lock:
                compression_jobs[job_id] = {
                    'job_id': job_id,
                    'status': 'uploaded',
                    'progress': 0,
                    'message': f'⏳ {len(video_files)} videos ready',
                    'zip_path': zip_path,
                    'extract_folder': extract_folder,
                    'video_files': video_files,
                    'filename': file.filename,
                    'file_type': 'zip',
                    'video_count': len(video_files),
                    'first_video_info': first_video_info,
                    'created_at': datetime.now().isoformat(),
                    'settings': {},
                    'videos_completed': 0
                }

            return jsonify({
                'job_id': job_id,
                'filename': file.filename,
                'file_type': 'zip',
                'video_count': len(video_files),
                'video_names': [os.path.basename(f) for f in video_files],
                'first_video_info': first_video_info
            }), 200

        else:
            return jsonify({'error': 'Invalid file type (ZIP or video format required)'}), 400

    except Exception as e:
        return jsonify({'error': f'Upload error: {str(e)}'}), 500

@app.route('/compress', methods=['POST'])
def compress():
    try:
        data = request.json
        job_id = data.get('job_id')
        settings = data.get('settings', {})

        with jobs_lock:
            if not job_id or job_id not in compression_jobs:
                return jsonify({'error': 'Invalid job ID'}), 400

            job = compression_jobs[job_id]
            if job['status'] not in ['uploaded']:
                return jsonify({'error': 'Job already processing'}), 400

            job['settings'] = settings
            job['status'] = 'queued'
            job['started_at'] = datetime.now().isoformat()

        # Add to queue
        job_queue.put((0, job_id))
        log_debug(job_id, f"📋 Queued")

        return jsonify({
            'job_id': job_id,
            'message': 'Added to compression queue',
            'status': 'queued',
            'active_compressions': len(active_threads),
            'pending_jobs': job_queue.qsize()
        }), 200

    except Exception as e:
        return jsonify({'error': f'Server error: {str(e)}'}), 500

@app.route('/status/<job_id>', methods=['GET'])
def get_status(job_id):
    with jobs_lock:
        if job_id not in compression_jobs:
            return jsonify({'error': 'Job not found'}), 404

        job = compression_jobs[job_id].copy()

        response = {
            'job_id': job_id,
            'status': job['status'],
            'progress': job['progress'],
            'message': job.get('message', ''),
            'file_type': job.get('file_type', ''),
            'active_compressions': len(active_threads),
            'pending_jobs': job_queue.qsize(),
            'max_concurrent': MAX_CONCURRENT_JOBS
        }

        if job['file_type'] == 'zip':
            response.update({
                'video_count': job.get('video_count', 0),
                'videos_completed': job.get('videos_completed', 0)
            })

        return jsonify(response), 200

@app.route('/download/<job_id>', methods=['GET'])
def download_file(job_id):
    with jobs_lock:
        if job_id not in compression_jobs:
            return jsonify({'error': 'Job not found'}), 404

        job = compression_jobs[job_id].copy()
        if job['status'] != 'completed':
            return jsonify({'error': 'Compression not completed'}), 400

        try:
            if job['file_type'] == 'single':
                output_path = job.get('output_path')
                output_filename = job.get('output_filename', 'compressed_video.mp4')

                if not output_path or not os.path.exists(output_path):
                    return jsonify({'error': 'File not found'}), 404

                return send_file(output_path, as_attachment=True, download_name=output_filename, mimetype='video/mp4')

            elif job['file_type'] == 'zip':
                # Create ZIP of all compressed videos
                output_zip = os.path.join(OUTPUT_FOLDER, f"compressed_{job_id}.zip")
                with zipfile.ZipFile(output_zip, 'w') as zipf:
                    for video_file in job['video_files']:
                        name_only = os.path.splitext(os.path.basename(video_file))[0]
                        compressed_file = os.path.join(OUTPUT_FOLDER, f"{name_only}_compressed.mp4")
                        if os.path.exists(compressed_file):
                            zipf.write(compressed_file, arcname=os.path.basename(compressed_file))

                return send_file(output_zip, as_attachment=True, download_name=f"compressed_{job_id}.zip", mimetype='application/zip')

        except Exception as e:
            return jsonify({'error': f'Download error: {str(e)}'}), 500

@app.route('/cleanup/<job_id>', methods=['DELETE'])
def cleanup_job(job_id):
    with jobs_lock:
        if job_id not in compression_jobs:
            return jsonify({'error': 'Job not found'}), 404

        try:
            job = compression_jobs[job_id]

            if job['file_type'] == 'single':
                if os.path.exists(job.get('input_path', '')):
                    os.remove(job['input_path'])
                if 'output_path' in job and os.path.exists(job['output_path']):
                    os.remove(job['output_path'])

            elif job['file_type'] == 'zip':
                if os.path.exists(job.get('extract_folder', '')):
                    shutil.rmtree(job['extract_folder'])
                if os.path.exists(job.get('zip_path', '')):
                    os.remove(job['zip_path'])
                # Clean compressed videos
                for video_file in job.get('video_files', []):
                    name_only = os.path.splitext(os.path.basename(video_file))[0]
                    compressed_file = os.path.join(OUTPUT_FOLDER, f"{name_only}_compressed.mp4")
                    if os.path.exists(compressed_file):
                        os.remove(compressed_file)
                # Clean output ZIP if exists
                output_zip = os.path.join(OUTPUT_FOLDER, f"compressed_{job_id}.zip")
                if os.path.exists(output_zip):
                    os.remove(output_zip)

            del compression_jobs[job_id]
            return jsonify({'message': 'Cleaned up'}), 200

        except Exception as e:
            return jsonify({'error': f'Cleanup error: {str(e)}'}), 500

@app.route('/queue-status', methods=['GET'])
def queue_status():
    return jsonify({
        'max_concurrent_jobs': MAX_CONCURRENT_JOBS,
        'active_compressions': len(active_threads),
        'pending_jobs': job_queue.qsize(),
        'total_jobs': len(compression_jobs),
        'cpu_cores': CPU_CORES,
        'timestamp': datetime.now().isoformat()
    }), 200

# =============== ERROR HANDLING ===============
@app.errorhandler(413)
def too_large(e):
    return jsonify({'error': 'File too large (max 10GB)'}), 413

@app.errorhandler(500)
def server_error(e):
    return jsonify({'error': 'Server error'}), 500

# =============== STARTUP ===============
if __name__ == '__main__':
    print(f"\n" + "="*70)
    print(f"🎬 VIDEO COMPRESSION TOOL - PARALLEL + ZIP SUPPORT")
    print(f"="*70)
    print(f"✅ Server: http://localhost:5000")
    print(f"✅ CPU Cores: {CPU_CORES}")
    print(f"✅ Max Concurrent Compressions: {MAX_CONCURRENT_JOBS}")
    print(f"✅ Supported: MP4, MOV, AVI, MKV, WMV, FLV, WEBM, MPEG, MPG, 3GP, ZIP")
    print(f"✅ Upload folder: ./uploads")
    print(f"✅ Output folder: ./compressed")
    print(f"="*70 + "\n")

    # Start background queue processor
    start_queue_processor()

    # Run Flask
    app.run(debug=True, host='0.0.0.0', port=5000, threaded=True)