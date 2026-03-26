import os, subprocess, tempfile, base64, json
from flask import Blueprint, request, jsonify, make_response

frames_bp = Blueprint("frames", __name__)

@frames_bp.route("/extract-frames", methods=["POST", "OPTIONS"])
def extract_frames():
    if request.method == "OPTIONS":
        resp = make_response("", 204)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        return resp

    if "video" not in request.files:
        return _cors({"error": "No video file"}, 400)

    file = request.files["video"]
    frame_count = int(request.form.get("frames", 8))

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input" + os.path.splitext(file.filename or ".mp4")[1])
        file.save(input_path)

        # Get duration
        probe = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", input_path
        ], capture_output=True, text=True)
        probe_data = json.loads(probe.stdout)
        duration = next(
            (float(s["duration"]) for s in probe_data.get("streams", []) if s.get("codec_type") == "video"),
            30.0
        )

        # Extract evenly spaced frames
        frames = []
        for i in range(frame_count):
            t = duration * (i + 0.5) / frame_count
            out_path = os.path.join(tmpdir, f"frame_{i}.jpg")
            subprocess.run([
                "ffmpeg", "-ss", str(t), "-i", input_path,
                "-vframes", "1",
                "-vf", "scale='min(768,iw)':-2",
                "-q:v", "3", "-y", out_path
            ], capture_output=True)
            if os.path.exists(out_path):
                with open(out_path, "rb") as f:
                    b64 = base64.b64encode(f.read()).decode()
                mm, ss = int(t // 60), int(t % 60)
                frames.append({
                    "b64": b64,
                    "timeLabel": f"{mm}:{str(ss).zfill(2)}",
                    "timeIndex": i + 1
                })

    return _cors({"frames": frames, "duration": duration}, 200)

def _cors(data, status):
    resp = make_response(jsonify(data), status)
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    return resp
