"""Guard that one bubble-opacity value reaches idle, session and realtime cards.

The App persists a single ``ui/bubble_opacity`` value. The P4 renderer used to
apply it only from ``draw_session_queue()``, which realtime conversation
rendering never calls, and the realtime card was painted with opaque fills.
Keep the sync at the top of ``pet_p4_renderer_render()`` and make every card
path read it; text is drawn by the callers after the panel so it never fades.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SYNC = "g_bubble_opacity_percent = state && state->bubble_opacity"


def renderer_source():
    return (ROOT / "main/pet_p4_renderer.c").read_text()


def test_renderer_syncs_opacity_before_drawing_any_bubble():
    source = renderer_source()
    render = source.split("esp_err_t pet_p4_renderer_render(", 1)[1]
    assert SYNC in render
    # The sync must happen before the first bubble/card paint of the frame.
    assert render.index(SYNC) < render.index("draw_bubble(")
    # draw_session_queue() no longer owns the only sync point.
    queue = source.split("static void draw_session_queue(", 1)[1].split(
        "typedef struct", 1)[0]
    assert SYNC not in queue


def test_idle_session_and_realtime_cards_share_the_opacity_panel():
    source = renderer_source()
    idle = source.split("static void draw_home_white_panel(", 1)[1].split(
        "static void rgb565_components(", 1)[0]
    assert "if (g_bubble_opacity_percent >= 100)" in idle
    assert "blend_round_rect(" in idle

    panel = source.split("static void draw_selected_card_panel(", 1)[1].split(
        "static void draw_session_card_panel(", 1)[0]
    assert "if (g_bubble_opacity_percent >= 100)" in panel
    assert "ppa_blend_card_surface(" in panel
    assert "blend_round_rect(" in panel
    # Text is painted by the caller after the panel, so it never fades.
    assert "draw_text" not in panel

    session = source.split("static void draw_session_card_panel(", 1)[1].split(
        "static void draw_card_body_lines(", 1)[0]
    assert "draw_selected_card_panel(" in session

    bubble = source.split("static void draw_bubble(", 1)[1].split(
        "static void draw_touch_feedback(", 1)[0]
    assert "realtime ? rgb565(42, 119, 225)" in bubble
    realtime_panel = bubble.index(
        "draw_selected_card_panel(x, y, w, h, PET_P4_SESSION_CARD_RADIUS,")
    assert "rgb565(170, 205, 250)" in bubble
    # Opaque title/body pixels are painted only after the alpha card surface.
    assert realtime_panel < bubble.index("pet_bubble_pixels_draw(PET_BUBBLE_TITLE")
    # The realtime card must not fall back to the old opaque fill stack.
    assert "fill_round_rect(x, y, w, h, PET_P4_SESSION_CARD_RADIUS, selection);" not in bubble
