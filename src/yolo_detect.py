from pathlib import Path
import csv
from ultralytics import YOLO

IMAGE_DIR = Path("data/raw/images")
OUTPUT_CSV = Path("data/yolo_detections.csv")

MODEL = YOLO("yolov8n.pt")


def classify_image(detected: set[str]) -> str:
    """
    Simple rule-based categorization using pre-trained YOLO classes.
    """
    has_person = "person" in detected
    has_any_other = len(detected - {"person"}) > 0

    if has_person and has_any_other:
        return "promotional"
    if (not has_person) and len(detected) > 0:
        return "product_display"
    if has_person and (not has_any_other):
        return "lifestyle"
    return "other"


def main() -> None:
    rows = []
    image_paths = list(IMAGE_DIR.rglob("*.jpg"))

    for i, img_path in enumerate(image_paths, start=1):
        channel = img_path.parent.name
        message_id = img_path.stem

        result = MODEL(img_path, verbose=False)[0]

        detected_classes = set()
        max_conf = 0.0

        if result.boxes is not None and len(result.boxes) > 0:
            for box in result.boxes:
                cls_name = MODEL.names[int(box.cls)]
                conf = float(box.conf)
                detected_classes.add(cls_name)
                max_conf = max(max_conf, conf)

        category = classify_image(detected_classes)

        rows.append([
            message_id,
            channel,
            ",".join(sorted(detected_classes)),
            round(max_conf, 3),
            category,
            str(img_path).replace("\\", "/"),
        ])

        # progress log every 50 images
        if i % 50 == 0:
            print(f"Processed {i}/{len(image_paths)} images...")

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "message_id",
            "channel_name",
            "detected_objects",
            "confidence_score",
            "image_category",
            "image_path",
        ])
        writer.writerows(rows)

    print(f"Saved {len(rows)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
