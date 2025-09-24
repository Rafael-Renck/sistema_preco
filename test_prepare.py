import io
from reportlab.platypus import Image
from reportlab.lib.units import mm

logo_max_height = 18 * mm

def _prepare(img_obj):
    try:
        height = float(img_obj.imageHeight)
        width = float(img_obj.imageWidth)
    except Exception as exc:
        print('prepare failed', exc)
        return None
    if height <= 0 or width <= 0:
        print('bad size', height, width)
        return None
    scale = logo_max_height / height
    img_obj.drawHeight = logo_max_height
    img_obj.drawWidth = width * scale
    img_obj.hAlign = 'LEFT'
    return img_obj

candidate = Image('static/logo-menu.png')
result = _prepare(candidate)
print('result', bool(result))
print('drawHeight', getattr(result, 'drawHeight', None))
