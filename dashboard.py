"""
ERANDA V2 — Dashboard
Dash UI — Redis'ten okur, kullanıcıya gösterir.
Collector çalışmadan dashboard anlamsız.

Multi-user: Her kullanıcı kendi coin/interval/indikatör seçimini yapar.
Collector tüm verileri hesaplar, dashboard sadece seçileni filtreler.

Çalıştır: python3 dashboard.py
"""

from shared import *
import json
from bus import KNS_PRESETS, kns_col_prefix, match_kns_preset, consensus_from_row
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash import Dash, dcc, html
from dash import dash_table

app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "ERANDA"

# ── Kontrol stilleri (koyu tema + mavi vurgu, GitHub-dark tonları) ────────────
_INPUT_STYLE = {
    'backgroundColor': '#161b22', 'color': '#e6edf3',
    'border': '1px solid rgba(31,182,220,0.45)', 'borderRadius': '8px',
    'padding': '9px 14px', 'fontSize': '14px', 'marginRight': '10px',
    'outline': 'none', 'minWidth': '220px', 'height': '38px',
    'boxSizing': 'border-box',
    'transition': 'border-color .15s, box-shadow .15s',
}
_BTN_PRIMARY = {   # Add Symbol — diğer butonlarla tutarlı koyu/çerçeveli
    'backgroundColor': '#0A0E11', 'color': '#4DD2F0',
    'border': '1px solid rgba(31,182,220,0.45)', 'borderRadius': '8px',
    'padding': '0 18px', 'height': '38px', 'fontSize': '14px',
    'fontWeight': '600', 'cursor': 'pointer', 'boxSizing': 'border-box',
    'transition': 'background-color .15s, border-color .15s',
}
_BTN_SECONDARY = {  # Update Data — ikincil (çerçeveli)
    'backgroundColor': '#0A0E11', 'color': '#4DD2F0',
    'border': '1px solid rgba(31,182,220,0.45)', 'borderRadius': '8px',
    'padding': '0 18px', 'height': '38px', 'fontSize': '14px',
    'fontWeight': '600', 'cursor': 'pointer',
    'marginTop': '10px', 'marginRight': '10px', 'boxSizing': 'border-box',
    'transition': 'background-color .15s, border-color .15s',
}
_BTN_TERTIARY = {   # Save Preferences — koyu kutulu, krom yazı (temaya uyumlu)
    'backgroundColor': '#0A0E11', 'color': '#4DD2F0',
    'border': '1px solid rgba(31,182,220,0.45)', 'borderRadius': '8px',
    'padding': '0 18px', 'height': '38px', 'fontSize': '14px',
    'fontWeight': '600', 'cursor': 'pointer', 'marginTop': '10px',
    'boxSizing': 'border-box',
    'transition': 'background-color .15s, border-color .15s, color .15s',
}

# Hover/focus efektleri (inline style hover desteklemez → CSS ile)
app.index_string = '''<!DOCTYPE html>
<html>
<head>
    {%metas%}<title>{%title%}</title>{%favicon%}{%css%}
    <style>
        #new-symbol-input::placeholder { color: #6e7681; }
        #new-symbol-input:focus {
            border-color: #1FB6DC !important;
            box-shadow: 0 0 0 3px rgba(31,182,220,.20) !important;
        }
        #add-symbol-button, #update-button, #save-button {
            border: 1px solid rgba(31,182,220,0.45) !important;
            border-radius: 8px !important;
            background-color: #0A0E11 !important;
            color: #4DD2F0 !important;
        }
        #add-symbol-button:hover { background-color: #0d2730 !important; border-color: #1FB6DC !important; color: #4DD2F0 !important; }
        #add-symbol-button:active { transform: translateY(1px); }
        #update-button:hover { background-color: #0d2730 !important; border-color: #1FB6DC !important; color: #4DD2F0 !important; }
        #save-button:hover { background-color: #0d2730 !important; border-color: #1FB6DC !important; color: #4DD2F0 !important; }

        /* Dropdown kutusu (kontrol) + menü — koyu, logo zemini */
        .Select-control { background-color: #0A0E11 !important; border: 1px solid rgba(190,200,210,0.28) !important; border-radius: 8px !important; }
        .Select.is-focused .Select-control { border-color: #1FB6DC !important; }
        .Select-menu-outer { background-color: #0C1116 !important; border-color: rgba(190,200,210,0.14) !important; }
        .Select-option { background-color: #0C1116 !important; color: #C2CCD3 !important; }
        .Select-option.is-focused { background-color: rgba(31,182,220,0.15) !important; color: #4DD2F0 !important; }
        .Select-placeholder, .Select--single > .Select-control .Select-value { color: #6E7A82 !important; }
        .Select-input > input { color: #C2CCD3 !important; }
        .Select-arrow { border-color: #6E7A82 transparent transparent !important; }
        .Select-clear { color: #6E7A82 !important; }

        /* Cyan çipler: sembol + interval + indikatör + KNS (hepsi logo rengi) */
        #symbol-dropdown .Select-value, #interval-dropdown .Select-value, #indicator-dropdown .Select-value, #kns-indicator-dropdown .Select-value {
            background-color: rgba(31,182,220,0.10) !important;
            border: 1px solid rgba(31,182,220,0.45) !important;
            color: #4DD2F0 !important; border-radius: 7px !important;
        }
        #symbol-dropdown .Select-value-label, #interval-dropdown .Select-value-label, #indicator-dropdown .Select-value-label, #kns-indicator-dropdown .Select-value-label { color: #4DD2F0 !important; }
        #symbol-dropdown .Select-value-icon, #interval-dropdown .Select-value-icon, #indicator-dropdown .Select-value-icon, #kns-indicator-dropdown .Select-value-icon {
            border-right: 1px solid rgba(31,182,220,0.35) !important; color: #1FB6DC !important;
        }
        #symbol-dropdown .Select-value-icon:hover, #interval-dropdown .Select-value-icon:hover, #indicator-dropdown .Select-value-icon:hover, #kns-indicator-dropdown .Select-value-icon:hover {
            background-color: rgba(31,182,220,0.22) !important; color: #4DD2F0 !important;
        }
    </style>
</head>
<body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body>
</html>'''

# ── Başlangıç seçimleri — layout render edilmeden önce tanımlanmalı ───────────
# Menüde gösterilen / veri üreten geçerli indikatörler (tek doğru kaynak)
VALID_INDICATORS = [
    'RSI', 'RSM', 'MFI', 'SMI', 'CMF', 'ADO', 'WIL', 'ADX', '+DI', 'CCI',
    'VWAP', 'CVD', 'VOL',
    'EMA', 'MCD', 'SMA_20', 'SMA_50', 'SMA_200', 'BOL', 'OBV', 'VRT', 'FRC',
    'TRP', 'TRD', 'KNS', 'SR',
]
_syms, _ivs, _inds = load_setup()
selected_symbols    = _syms if _syms else ["BTCUSDT", "ETHUSDT"]
selected_intervals  = _ivs  if _ivs  else ["1m", "5m", "15m", "1h"]
# Kayıtlı seçimden artık geçersiz (TPD/Fibonacci vb.) olanları ele
_inds = [i for i in (_inds or []) if i in VALID_INDICATORS]
selected_indicators = _inds if _inds else ["RSI", "RSM", "MFI", "SMI", "CMF", "ADO", "+DI", "CCI", "WIL"]
kns_indicators_global = ["VOL", "CVD", "VWAP"]

# ── Dashboard in-memory cache ─────────────────────────────────────────────────
# Collector'ın Redis'e yazdığı veriden seçilen coin/interval'ları çeker
# Her 250ms'de Dash callback Redis'ten günceller
cache      = defaultdict(lambda: deque(maxlen=CACHE_SIZE))
cache_lock = threading.Lock()

def get_from_collector(symbol: str, interval: str):
    """Redis'ten taze indikatörlü df çek — her callback'te güncellenir."""
    try:
        result = redis_cache.get_dataframe(symbol, interval)
        if result and isinstance(result, tuple):
            df, _ = result
            if df is not None and not df.empty:
                return df
    except Exception:
        pass
    return None


def _live_overlay(symbol: str, interval: str):
    """Consumer'ın yazdığı hafif 'live:SEMBOL:interval' son-bar JSON'unu oku.
    Forming mumu saniye-altı güncel tutar; yoksa None (snapshot kullanılır)."""
    try:
        raw = redis_cache.client.get(f"live:{symbol}:{interval}")
        if not raw:
            return None
        return json.loads(raw)
    except Exception:
        return None

app.layout = html.Div([
    html.Div([
        html.Img(src='/assets/eranda_logo.png', style={
            'height': '84px', 'width': 'auto', 'marginRight': '18px',
            'objectFit': 'contain'
        }),
        html.Div([
            html.H1("ERANDA", style={
                'color': '#e6edf3', 'margin': '0', 'fontSize': '34px',
                'fontWeight': '700', 'letterSpacing': '6px', 'fontFamily': 'system-ui, sans-serif'
            }),
            html.Div("Real-Time Market Analytics System", style={
                'color': '#1FB6DC', 'fontSize': '13px', 'letterSpacing': '2px',
                'marginTop': '2px', 'textTransform': 'uppercase', 'fontFamily': 'system-ui, sans-serif'
            }),
        ]),
    ], style={'display': 'flex', 'alignItems': 'center', 'marginBottom': '20px'}),
    html.Div([
        dcc.Input(
            id='new-symbol-input',
            type='text',
            placeholder='Enter new symbol',
            style=_INPUT_STYLE,
        ),
        html.Button('Add Symbol', id='add-symbol-button', n_clicks=0, style=_BTN_PRIMARY),
    ], style={'marginBottom': '10px', 'display': 'flex', 'alignItems': 'center'}),
    html.Div([
        dcc.Dropdown(
            id='symbol-dropdown',
            options=[{'label': s, 'value': s} for s in COLLECTOR_SYMBOLS],
            value=selected_symbols,
            multi=True,
            style={'backgroundColor': 'transparent'}
        ),
        dcc.Dropdown(
            id='interval-dropdown',
            options=[{'label': i, 'value': i} for i in COLLECTOR_INTERVALS],
            value=selected_intervals,
            multi=True,
            style={'backgroundColor': 'transparent'}
        ),
        dcc.Dropdown(
            id='indicator-dropdown',
            options=[{'label': i, 'value': i} for i in VALID_INDICATORS],
                                                         
            value=selected_indicators,
            multi=True,
            style={'backgroundColor': 'transparent'}
        ),
        html.Button('Update Data', id='update-button', n_clicks=0, style=_BTN_SECONDARY),
        html.Button('Save Preferences', id='save-button', n_clicks=0, style=_BTN_TERTIARY),
        html.Div([
            html.Div('⚡ KNS Combination', style={
                'color': '#1FB6DC', 'fontSize': '12px', 'marginTop': '10px',
                'marginBottom': '4px', 'letterSpacing': '1px'
            }),
            dcc.Dropdown(
                id='kns-indicator-dropdown',
                options=[{'label': i, 'value': i} for i in [
                    'TRD', 'RSI', 'MFI', 'SMI', 'CMF', 'ADO', 'WIL',
                    'ADX', 'MCD', 'EMA', 'CCI', 'CVD', 'VOL', 'VWAP',
                    'OBV', 'VRT', 'FRC', 'BOL', 'RSM', 'TRP',
                    'SMA_20', 'SMA_50', 'SMA_200'
                ]],
                value=['VOL', 'CVD', 'VWAP'],
                multi=True,
                placeholder='Select indicators for KNS...',
                style={'backgroundColor': 'transparent', 'minWidth': '400px'}
            ),
        ], style={'marginTop': '6px'}),
    ], style={'marginBottom': '20px'}),
    dcc.Interval(
        id='interval-component',
        interval=1000,
        n_intervals=0
    ),
    dcc.Store(id='grid-store'),
    html.Div(id='cs-dummy', style={'display': 'none'}),
    html.Div(id='live-update-text',
             style={'color': 'white', 'fontFamily': 'monospace'})
], style={'backgroundColor': '#000000', 'padding': '20px'})


# ── Clientside render: grid-store JSON → tek innerHTML yazımı ─────────────────
# Sunucu kompakt JSON üretir; tarayıcı tüm component ağacını React ile yeniden
# kurmaz, tek string parse eder → çok sembol/interval'da donmaz, hızlıdır.
app.clientside_callback(
    """
    function(data) {
        var el = document.getElementById('live-update-text');
        if (!el || !data) { return ''; }
        if (data.error) {
            el.innerHTML = '<div style="color:orange;font-family:monospace">Render error: '
                + data.error + '</div>';
            return '';
        }
        function esc(s){ return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
        var h = '';
        if (data.stale) {
            h += '<div style="background:#3d1418;border:1px solid #f85149;color:#ff7b72;'
                + 'padding:10px 14px;border-radius:8px;margin-bottom:10px;font-weight:600">'
                + '⚠ Data flow has stopped.</div>';
        }
        var statusColor = data.stale ? '#8b949e' : '#2BC4E8';
        var statusDot = data.stale ? '○' : '●';
        h += '<div style="color:' + statusColor + ';font-family:monospace;margin-bottom:8px">'
            + statusDot + ' live — ' + esc(data.time) + ' | ' + data.nsym
            + ' symbols × ' + data.niv + ' intervals</div>';
        var groups = data.groups || [];
        for (var g=0; g<groups.length; g++) {
            h += '<div style="border-bottom:1px solid #333;padding-bottom:10px;margin-bottom:10px">';
            var rows = groups[g].rows || [];
            for (var r=0; r<rows.length; r++) {
                var row = rows[r];
                h += '<div style="margin-bottom:10px"><b>' + esc(row.label) + '</b>';
                var cells = row.cells || [];
                for (var c=0; c<cells.length; c++) {
                    var cell = cells[c];
                    var cls = cell.f ? (' class="' + cell.f + '"') : '';
                    h += '<span style="color:' + (cell.c || 'white') + '"' + cls + '>'
                        + esc(cell.t) + '</span>';
                }
                h += '</div>';
            }
            h += '</div>';
        }
        el.innerHTML = h;
        return '';
    }
    """,
    Output('cs-dummy', 'children'),
    Input('grid-store', 'data'),
)


@app.callback(
    Output('save-button', 'n_clicks'),
    [Input('save-button', 'n_clicks')],
    [State('symbol-dropdown', 'value'),
     State('interval-dropdown', 'value'),
     State('indicator-dropdown', 'value')]
)
def save_setup_callback(n_clicks, symbols, intervals, indicators):
    if n_clicks > 0:
        save_setup(symbols, intervals, indicators) 
    return 0


@app.callback(
    Output('grid-store', 'data'),
    [Input('interval-component', 'n_intervals')],
    [State('symbol-dropdown', 'value'),
     State('interval-dropdown', 'value'),
     State('indicator-dropdown', 'value'),
     State('kns-indicator-dropdown', 'value')]
)
def update_output(n, selected_symbols, selected_intervals, selected_indicators, kns_indicators):
    global kns_indicators_global
    kns_indicators = kns_indicators or ['VOL', 'CVD', 'VWAP']
    kns_indicators_global = kns_indicators

    def shorten(value):
        try:
            v = float(value)
            if abs(v) >= 1_000_000: return f"{v/1_000_000:.1f}M"
            if abs(v) >= 1_000:     return f"{v/1_000:.1f}K"
            return f"{v:.2f}"
        except Exception:
            return str(value)
    try:
        if not selected_symbols or not selected_intervals or not selected_indicators:
            raise PreventUpdate

        output = []
        import datetime as _dt
        _loaded = 0
        # Render-başına memo: aynı (sembol, interval) df'i her indikatör için
        # tekrar tekrar Redis'ten deserialize etme. 351 okuma → 39'a iner.
        _rc = {}    # df cache (sym,iv) — sadece nadir df-gerektiren dal (Stoch) için
        _src = {}   # snap: son-satır JSON cache (sym,iv)
        _lrc = {}   # son-satır (overlay'li) cache (sym,iv)
        def _get_df(_sym, _iv):
            """Tam 500-satır df (lazy). Normal indikatörler buna ihtiyaç duymaz;
            sadece çok-satır gerektiren nadir dallar (Stoch) için."""
            _k = (_sym, _iv)
            if _k not in _rc:
                _rc[_k] = get_from_collector(_sym, _iv)
            return _rc[_k]
        def _get_snap(_sym, _iv):
            """Consumer'ın yazdığı KÜÇÜK son-satır JSON'u ('snap:'). 500-satır df
            yerine bunu okur → dashboard çok sembol/kullanıcıda boğulmaz."""
            _k = (_sym, _iv)
            if _k not in _src:
                try:
                    raw = redis_cache.client.get(f"snap:{_sym}:{_iv}")
                    _src[_k] = json.loads(raw) if raw else None
                except Exception:
                    _src[_k] = None
            return _src[_k]
        def _get_last_row(_sym, _iv):
            """snap: (kapalı/snapshot son bar) + live: overlay (forming bar).
            (sym,iv) başına bir kez. Tam df HİÇ açılmaz."""
            _k = (_sym, _iv)
            if _k not in _lrc:
                _snap = _get_snap(_sym, _iv)
                if not _snap:
                    _lrc[_k] = None
                else:
                    _lr = dict(_snap)
                    _ov = _live_overlay(_sym, _iv)
                    if _ov:
                        for _c, _v in _ov.items():
                            if _v is not None:
                                _lr[_c] = _v
                    _lrc[_k] = _lr
            return _lrc[_k]
        for symbol in selected_symbols:
            symbol_data = []
            
            for indicator in selected_indicators:
                indicator_data = [html.B(f"{symbol} {indicator}:")]
                
                for interval in selected_intervals:
                    # snap: + live: küçük JSON'lardan son satır (tam df açılmaz)
                    last_row = _get_last_row(symbol, interval)
                    if last_row is None:
                        indicator_data.append(
                            html.Span(f" {interval}: No data", style={'color': 'gray'})
                        )
                        continue

                    if indicator == 'CMF':
                        cmf = last_row.get('CMF', 0)
                        color = 'red'; class_name = ''
                        if cmf > 0.30: color = 'blue'
                        elif cmf > 0:  color = 'green'
                        if -100 <= cmf <= -42: class_name = 'flash'
                        indicator_data.append(html.Span(
                            f" {interval}: {cmf:.2f}",
                            style={'color': color}, className=class_name))

                    elif indicator == 'MFI':
                        mfi = last_row.get('MFI', 0)
                        color = 'red'  # Varsayılan renk
                        class_name = ''  # Varsayılan sınıf

                        if mfi > 80:
                            color = 'blue'  # Aşırı alım (blue)
                        elif mfi > 50:
                            color = 'green'  # Orta seviyede (green)
                        elif mfi < 50:
                            color = 'red'  # Aşırı satım (red)

                        # Animasyon için belirli bir aralık
                        if 1 <= mfi <= 20:
                            class_name = 'flash'  # MFI 10 ile 15 arasındaysa animasyon ekle

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {mfi:.2f}",
                                style={'color': color},
                                className=class_name
                            )
                        )

                    elif indicator in ['+DI', '-DI']:
                        di_plus = last_row.get('+DI', 0)
                        di_minus = last_row.get('-DI', 0)
                        net_di = di_plus - di_minus

                        # Varsayılan renk ve sınıf
                        color = 'green' if net_di > 10 else 'red' if net_di < 7 else 'green'
                        class_name = ''

                        # -15 ile -30 arası için flash efekti
                        if -30 <= net_di <= -15:
                            class_name = 'flash'

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {net_di:.2f}",
                                style={'color': color},
                                className=class_name
                            )
                        )

                    elif indicator == 'ADO':
                        ado_value = last_row.get('ADO', 0)
                        shortened_ado = shorten(ado_value)  # Kısaltılmış değer
                        color = 'green' if ado_value > 0 else 'red'
                        indicator_data.append(
                            html.Span(f" {interval}: {shortened_ado}", style={'color': color})
                        )

                    elif indicator == 'WIL':
                        wıl = last_row.get('WIL', 0)
                        class_name = ""  # Varsayılan olarak yanıp sönme yok

                        if wıl > -20:
                            color = 'blue'  # -20'den büyükse mavi
                        elif -80 < wıl <= -20:
                            color = 'green'  # -20 ile -80 arasında ise yeşil
                        elif -100 <= wıl <= -90:  # -90 ile -100 arasında ise yanıp sönme
                            color = 'red'
                            class_name = "flash"  # Yanıp sönme sınıfı
                        else:  # -80'den küçükse kırmızı
                            color = 'red'

                        indicator_data.append(
                            html.Span(f" {interval}: {wıl:.2f}", style={'color': color}, className=class_name)
                        )

                    elif indicator == 'CVD':
                        cvd_value = last_row.get('CVD', 0)
                        cvd_mom   = last_row.get('CVD_Mom', None)
                        cvd_slope = last_row.get('CVD_Slope', None)

                        try:
                            cvd_mom   = float(cvd_mom)   if cvd_mom   is not None else None
                            cvd_slope = float(cvd_slope) if cvd_slope is not None else None
                            if cvd_mom   is not None and pd.isna(cvd_mom):   cvd_mom   = None
                            if cvd_slope is not None and pd.isna(cvd_slope): cvd_slope = None
                        except:
                            cvd_mom = cvd_slope = None

                        # Renk: momentum yönüne göre (anlık baskı)
                        if cvd_mom is not None:
                            if cvd_mom > 0 and (cvd_slope or 0) > 0:
                                color = '#00ff88'   # güçlü alış — parlak yeşil
                                class_name = ''
                            elif cvd_mom > 0:
                                color = 'green'     # alış ama yavaşlıyor
                                class_name = ''
                            elif cvd_mom < 0 and (cvd_slope or 0) < 0:
                                color = 'red'       # güçlü satış
                                class_name = 'flash'
                            else:
                                color = '#ff8888'   # satış ama yavaşlıyor
                                class_name = ''
                        else:
                            color = 'green' if cvd_value > 0 else 'red'
                            class_name = ''

                        # Momentum oku
                        if cvd_slope is not None:
                            trend_arrow = '↑' if cvd_slope > 0 else '↓'
                        else:
                            trend_arrow = ''

                        shortened_cvd = shorten(cvd_value)
                        mom_str = shorten(cvd_mom) if cvd_mom is not None else ''

                        tooltip = (
                            f"CVD (kümülatif): {shortened_cvd}\n"
                            f"Momentum (20 mum): {mom_str}\n"
                            f"Slope (5 mum): {shorten(cvd_slope) if cvd_slope else '—'}"
                        )

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {shortened_cvd}{trend_arrow}",
                                style={'color': color, 'fontWeight': 'bold'},
                                className=class_name,
                                title=tooltip
                            )
                        )

                    elif indicator == 'VWAP':
                        vwap = last_row.get('VWAP', None)
                        try:
                            vwap = float(vwap)
                            if pd.isna(vwap):
                                vwap = None
                        except:
                            vwap = None

                        close = last_row.get('close', None)
                        try:
                            close = float(close)
                        except:
                            close = None

                        if vwap and close:
                            arrow = '▲' if close > vwap else '▼'
                            color = 'green' if close > vwap else 'red'
                            pct_diff = ((close - vwap) / vwap * 100)

                            # Akıllı format
                            if vwap >= 100:
                                fmt = f"{vwap:.2f}"
                            elif vwap >= 1:
                                fmt = f"{vwap:.4f}"
                            else:
                                fmt = f"{vwap:.6f}"

                            indicator_data.append(
                                html.Span(
                                    f" {interval}: {arrow}{fmt}",
                                    style={'color': color, 'fontWeight': 'bold'},
                                    title=f"VWAP: {fmt}\nFiyat: {close}\nFark: {pct_diff:+.2f}%"
                                )
                            )
                        else:
                            indicator_data.append(
                                html.Span(f" {interval}: VWAP yok", style={'color': 'gray'})
                            )


                      # In the OBV/Net Volume section
                    elif indicator == 'VOL':
                        net_volume_value = last_row.get('Net_Volume', 0)
                        net_vol_z = last_row.get('Net_Volume_Z', None)
                        try:
                            net_vol_z = float(net_vol_z) if net_vol_z is not None else None
                            if pd.isna(net_vol_z):
                                net_vol_z = None
                        except:
                            net_vol_z = None

                        class_name = ''
                        # Z-score eşikleri: ±2 = aşırı, ±2.5 = çok aşırı
                        if net_vol_z is not None:
                            if net_vol_z <= -2.0:   # Aşırı satım baskısı → yanıp sönen kırmızı
                                color = 'red'
                                class_name = 'flash'
                            elif net_vol_z >= 2.0:  # Aşırı alım baskısı → yanıp sönen yeşil
                                color = '#00ff88'
                                class_name = 'flash'
                            elif net_volume_value > 0:
                                color = 'green'
                            elif net_volume_value < 0:
                                color = 'red'
                            else:
                                color = 'gray'
                        else:
                            color = 'green' if net_volume_value > 0 else ('red' if net_volume_value < 0 else 'gray')

                        shortened_net_volume = shorten(abs(net_volume_value))
                        sign = '-' if net_volume_value < 0 else ('+' if net_volume_value > 0 else '')

                        # Z-score varsa tooltip'te göster
                        z_str = f" (z:{net_vol_z:.1f})" if net_vol_z is not None else ""
                        tooltip = f"Net Vol: {sign}{shortened_net_volume}\nZ-score: {net_vol_z:.2f}" if net_vol_z is not None else ""

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {sign}{shortened_net_volume}",
                                style={'color': color, 'fontWeight': 'bold'},
                                className=class_name,
                                title=tooltip
                            )
                        )

                    elif indicator == 'SMI':
                        smi = last_row.get('SMI', 0)
                        color = 'red'
                        class_name = ''
                        if smi > 75:
                            color = 'blue'  # 75'den büyükse sarı
                        elif smi > 0:
                            color = 'green'  # 50 ile 75 arasında ise yeşil
                        elif smi < 50:
                            color = 'red'  # 35'den küçükse sarı
                        else:
                            color = 'red'  # 50'den küçükse kırmızı
                        if -100 <= smi <= -40:
                            class_name = 'flash'  # MFI 10 ile 15 arasındaysa animasyon ekle
                        indicator_data.append(
                            html.Span(f" {interval}: {smi:.2f}", style={'color': color},
                                      className=class_name
                            )
                        )



                    elif indicator == 'RSI':
                        rsi = last_row.get('RSI', 0)
                        color = 'red'  # Varsayılan renk
                        class_name = ''  # Varsayılan sınıf
                        if rsi > 75:
                            color = 'blue'  # 75'den büyükse sarı
                        elif rsi > 50:
                            color = 'green'  # 50 ile 75 arasında ise yeşil
                        elif rsi < 35:
                             class_name = 'flash'  # CSS'deki animasyon sınıfı atanır
                        indicator_data.append(
                            html.Span(
                                f" {interval}: {rsi:.2f}",
                                style={'color': color},
                                className=class_name
                            )
                        )

                    elif indicator == 'SMA_20':
                        sma_20 = last_row.get('SMA_20', 0)
                        color = 'green' if last_row['close'] > sma_20 else 'red'
                        indicator_data.append(
                            html.Span(f" {interval}: {sma_20:.2f}", style={'color': color})
                        )

                    # SMA_50
                    elif indicator == 'SMA_50':
                        sma_50 = last_row.get('SMA_50', 0)
                        color = 'green' if last_row['close'] > sma_50 else 'red'
                        indicator_data.append(
                            html.Span(f" {interval}: {sma_50:.2f}", style={'color': color})
                        )

                    elif indicator == 'SMA_200':
                        sma_200 = last_row.get('SMA_200', 0)
                        color = 'green' if last_row['close'] > sma_200 else 'red'
                        indicator_data.append(
                            html.Span(f" {interval}: {sma_200:.2f}", style={'color': color})
                        )

                    elif indicator == 'ADX':
                        adx = last_row.get('ADX')
                        if adx is not None:
                            if adx < 20:
                                color = 'red'
                            elif 20 <= adx < 40:
                                color = 'red'
                            else:
                                color = 'green'
                            indicator_data.append(html.Span(f" {interval}: {adx:.2f}", style={'color': color}))
                        else:
                            indicator_data.append(html.Span(f" {interval}: No data", style={'color': 'gray'}))

                    elif indicator == 'OBV':
                        obv_value = last_row.get('OBV', 0)
                        color = 'green' if obv_value > 0 else 'red' 
                        shortened_obv = shorten(obv_value)  # Kısaltılmış değer
                        indicator_data.append(
                            html.Span(f" {interval}: {shortened_obv}", style={'color': color}))

                    elif indicator == 'Stoch':
                        df = _get_df(symbol, interval)
                        if df is None or df.empty:
                            indicator_data.append(html.Span(f" {interval}: No data", style={'color': 'gray'}))
                            continue
                        stoch = ta.momentum.StochasticOscillator(df['high'], df['low'], df['close'])
                        k = stoch.stoch().iloc[-1]
                        d = stoch.stoch_signal().iloc[-1]
                        color = 'green' if k > d else 'red'
                        indicator_data.append(html.Span(f" {interval}: K:{k:.2f} D:{d:.2f}", style={'color': color}))


                    elif indicator == 'MCD':
                        mcd = last_row.get('MCD', 0)
                        mcd_sinyal = last_row.get('MCD_Signal', 0)
                        mcd_hist = last_row.get('MCD_Hist', 0)
                        color = 'green' if mcd > 0 else 'red'
                        indicator_data.append(html.Span(f" {interval}:  {mcd:.2f}", style={'color': color}))

                    elif indicator == 'VRT':
                        vi_plus = last_row.get('VI+', 0)
                        vi_minus = last_row.get('VI-', 0)
                        net_vi = vi_plus - vi_minus
                        color = 'green' if net_vi > 0 else 'red'
                        indicator_data.append(
                            html.Span(f" {interval}:  {net_vi:.2f}", style={'color': color})
                        )

                    elif indicator == 'FRC':
                        frc = last_row.get('FRC', 0)
                        color = 'green' if frc > 0 else 'red'
                        indicator_data.append(
                            html.Span(f" {interval}: {frc:.2f}", style={'color': color})
                        )

                    elif indicator == 'Fibonacci':

                        # Değerleri güvenli float'a çevir
                        def _safe_float(v):
                            try:
                                f = float(v)
                                return None if pd.isna(f) else f
                            except:
                                return None

                        # Fiyatı otomatik formatlayan fonksiyon
                        def _fmt(v):
                            if v is None:
                                return '—'
                            if v >= 100:
                                return f"{v:.0f}"
                            elif v >= 1:
                                return f"{v:.2f}"
                            elif v >= 0.01:
                                return f"{v:.4f}"
                            else:
                                return f"{v:.6f}"

                        close_price = _safe_float(last_row.get('close', 0))
                        fib_high    = _safe_float(last_row.get('Fib_High', None))
                        fib_low     = _safe_float(last_row.get('Fib_Low',  None))

                        fib_level_defs = [
                            ('0%',    'Fib_0',    '#1E90FF'),
                            ('23.6%', 'Fib_236',  '#32CD32'),
                            ('38.2%', 'Fib_382',  '#90EE90'),
                            ('50%',   'Fib_500',  '#FFD700'),
                            ('61.8%', 'Fib_618',  '#FF8C00'),
                            ('78.6%', 'Fib_786',  '#FF4444'),
                            ('100%',  'Fib_1000', '#CC00CC'),
                        ]

                        # Fib_High/Low eksikse doğrudan df'den hesapla (12h gibi durumlar için)
                        if close_price and (fib_high is None or fib_low is None):
                            try:
                                fib_dict = calculate_fibonacci_retracement(df)
                                if fib_dict:
                                    fib_high = fib_dict.get('Fib_High')
                                    fib_low  = fib_dict.get('Fib_Low')
                                    for col, val in fib_dict.items():
                                        df[col] = val
                            except Exception:
                                pass

                        if close_price and fib_high and fib_low:
                            closest_label = None
                            closest_val   = None
                            closest_color = '#888888'
                            below_label   = None
                            below_val     = None
                            above_label   = None
                            above_val     = None
                            min_dist      = float('inf')

                            # 1. En yakın seviyeyi bul
                            for label, col, color in fib_level_defs:
                                val = _safe_float(last_row.get(col, None))
                                # Kolon df'de yoksa fib_dict'ten dene
                                if val is None:
                                    try:
                                        val = _safe_float(df[col].iloc[-1])
                                    except Exception:
                                        pass
                                if val is None:
                                    continue
                                dist = abs(close_price - val)
                                if dist < min_dist:
                                    min_dist      = dist
                                    closest_label = label
                                    closest_val   = val
                                    closest_color = color

                            # 2. Closest hariç, bir altı (destek) ve bir üstü (direnç) bul
                            for label, col, color in fib_level_defs:
                                if label == closest_label:
                                    continue
                                val = _safe_float(last_row.get(col, None))
                                if val is None:
                                    try:
                                        val = _safe_float(df[col].iloc[-1])
                                    except Exception:
                                        pass
                                if val is None:
                                    continue
                                if val < closest_val:   # closest'in altındaki en yakın
                                    if below_val is None or val > below_val:
                                        below_label, below_val = label, val
                                if val > closest_val:   # closest'in üstündeki en yakın
                                    if above_val is None or val < above_val:
                                        above_label, above_val = label, val

                            if closest_label:
                                # ▲ = fiyat closest'in üstünde veya üzerinde
                                arrow    = '▲' if close_price >= closest_val else '▼'
                                pct_dist = (abs(close_price - closest_val) / closest_val * 100) if closest_val else 0

                                support_str = f"S:{below_label}({_fmt(below_val)})"  if below_val else "S:—"
                                resist_str  = f"R:{above_label}({_fmt(above_val)})"  if above_val else "R:—"

                                tooltip = (
                                    f"Fiyat: {_fmt(close_price)}\n"
                                    f"En yakın Fib: {closest_label} @ {_fmt(closest_val)} ({pct_dist:.2f}% uzak)\n"
                                    f"Destek : {below_label} @ {_fmt(below_val)}\n"
                                    f"Direnç : {above_label} @ {_fmt(above_val)}\n"
                                    f"Dönem High: {_fmt(fib_high)}  Low: {_fmt(fib_low)}"
                                )

                                indicator_data.append(
                                    html.Span(
                                        f" {interval}: {arrow}{closest_label}({_fmt(closest_val)}) {support_str} {resist_str}",
                                        style={'color': closest_color, 'fontWeight': 'bold'},
                                        title=tooltip
                                    )
                                )
                            else:
                                indicator_data.append(
                                    html.Span(f" {interval}: No data", style={'color': 'gray'})
                                )
                        else:
                            indicator_data.append(
                                html.Span(f" {interval}: Fib hesaplanamadı", style={'color': 'gray'})
                            )

                    elif indicator == 'EMA':
                        ema_3 = last_row.get('EMA_3', 0)
                        ema_8 = last_row.get('EMA_8', 0)
                        ema_signal = last_row.get('EMA_Signal', 0)

                        # EMA crossover sinyali renklendirme
                        if ema_signal > 1:  # EMA3 > EMA8 (güçlü bullish)
                            color = 'green'
                            class_name = ''
                        elif ema_signal > 0:  # EMA3 > EMA8 (zayıf bullish)
                            color = 'green'
                            class_name = ''
                        elif ema_signal < -1:  # EMA3 < EMA8 (güçlü bearish)
                            color = 'red'
                            class_name = 'flash'  # Güçlü düşüş sinyali için flash
                        else:  # EMA3 < EMA8 (zayıf bearish)
                            color = 'red'
                            class_name = ''

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {ema_signal:.4f}",
                                style={'color': color, 'fontWeight': 'bold'},
                                className=class_name,
                                title=f"EMA3: {ema_3:.4f}, EMA8: {ema_8:.4f}"  # Hover'da detay göster
                            )
                        )

                    elif indicator == 'BOL':
                        middle = last_row.get('BB_Middle')
                        close = last_row.get('close')

                        if middle is not None:
                            direction = '↑' if close > middle else '↓'
                            color = 'green' if close > middle else 'red'
                            indicator_data.append(html.Span(
                                f" {interval}: {middle:.2f} {direction}",
                                style={'color': color}
                            ))
                        else:
                            indicator_data.append(html.Span(f" {interval}: No data", style={'color': 'gray'}))


                    elif indicator == 'CCI':
                        cci = last_row.get('CCI', 0)
                        color = 'gray'  # Varsayılan renk
                        class_name = ''  # Varsayılan sınıf

                        # CCI seviye renklendirme (scalp için optimize)
                        if cci > 200:  # Çok güçlü momentum yukarı
                            color = 'blue'
                            class_name = ''
                        elif cci > 100:  # Güçlü momentum yukarı
                            color = 'green'
                            class_name = ''
                        elif cci > 0:  # Zayıf momentum yukarı
                            color = 'green'
                            class_name = ''
                        elif cci > -100:  # Zayıf momentum aşağı
                            color = 'red'
                            class_name = ''
                        elif cci > -200:  # Güçlü momentum aşağı
                            color = 'red'
                            class_name = 'flash'  # Güçlü düşüş için flash
                        else:  # Çok güçlü momentum aşağı
                            color = 'red'
                            class_name = 'flash'

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {cci:.2f}",
                                style={'color': color, 'fontWeight': 'bold'},
                                className=class_name,
                                title=f"CCI: {cci:.2f} - Momentum göstergesi"
                            )
                        )

                    elif indicator == 'RSM':
                        rsm = last_row.get('RSM', 0)
                        color = 'red'  # Varsayılan renk
                        class_name = ''  # Varsayılan sınıf

                        if rsm > 75:
                            color = 'blue'
                        elif rsm > 50:
                            color = 'green'
                        elif rsm < 35:
                            class_name = 'flash'  # CSS'deki animasyon sınıfı atanır

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {rsm:.2f}",
                                style={'color': color},
                                className=class_name
                            )
                        )

                    elif indicator == 'TRP':
                        trp = last_row.get('TRP', 0)
                        color = 'gray'
                        class_name = ''

                        if trp > 50:
                            color = 'blue'  # Güçlü yukarı sinyali
                        elif 20 < trp <= 50:
                            color = 'green'  # Yükseliş eğilimi
                        elif -20 <= trp <= 20:
                            color = 'red'  # Nötr bölge
                        elif -50 <= trp < -20:
                            color = 'red'  # Düşüş eğilimi
                        elif trp < -50:
                            color = 'red'  # Güçlü düşüş

                        # TRP flash aralığı: -10 ile +10 arasında ise nötr ani değişim
                        if -50 <= trp <= -10:
                            class_name = 'flash'

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {trp:.2f}",
                                style={'color': color},
                                className=class_name
                            )
                        )

                    elif indicator == 'TPD_SIGNAL':
                        if 'TPD_Signal' in last_row:
                            signal = last_row.get('TPD_Signal', 'NEUTRAL')

                            if signal == 'STRONG_BUY':
                                color      = 'blue'
                                class_name = ''
                                icon       = '⬆⬆'
                            elif signal in ('BUY', 'MOMENTUM_REVERSAL_BUY'):
                                color      = 'green'
                                class_name = ''
                                icon       = '⬆'
                            elif signal == 'WEAK_BUY':
                                color      = '#00cc66'   # açık yeşil
                                class_name = ''
                                icon       = '↑'
                            elif signal == 'STRONG_SELL':
                                color      = 'red'
                                class_name = 'flash'
                                icon       = '⬇⬇'
                            elif signal in ('SELL', 'MOMENTUM_REVERSAL_SELL'):
                                color      = 'red'
                                class_name = ''
                                icon       = '⬇'
                            elif signal == 'WEAK_SELL':
                                color      = '#ff6666'   # açık kırmızı
                                class_name = ''
                                icon       = '↓'
                            else:                        # NEUTRAL
                                color      = '#888888'
                                class_name = ''
                                icon       = '↔'

                            indicator_data.append(
                                html.Span(
                                    f" {interval}: {icon} {signal}",
                                    style={'color': color, 'fontWeight': 'bold'},
                                    className=class_name
                                )
                            )

                    elif indicator == 'TPD_RELIABILITY':
                        if 'TPD_Reliability' in last_row:
                            reliability = last_row.get('TPD_Reliability', 0.0)
                            color = 'red' if reliability < 0.6 else 'green' if reliability < 0.8 else 'blue'
                            indicator_data.append(html.Span(f" {interval}: {reliability:.2f}", style={'color': color}))

                    elif indicator == 'TPD_MOMENTUM':
                        if 'TPD_Momentum' in last_row:
                            momentum = last_row.get('TPD_Momentum', 0.0)
                            color = 'red' if momentum <= 0 else 'green' if momentum <= 1 else 'blue'
                            indicator_data.append(html.Span(f" {interval}: {momentum:.2f}", style={'color': color}))

                    elif indicator == 'TPD_RISK':
                        if 'TPD_Risk' in last_row:
                            risk = last_row.get('TPD_Risk', 'MEDIUM')
                            color = (
                                'red' if risk == 'HIGH' else
                                'orange' if risk == 'MEDIUM' else
                                'green' if risk == 'LOW' else
                                '#888888'
                            )
                            indicator_data.append(html.Span(f" {interval}: {risk}", style={'color': color}))

                    elif indicator == 'TPD_CONFLUENCE':
                        if 'TPD_MTF_Confluence' in last_row:
                            confluence = last_row.get('TPD_MTF_Confluence', 0.0)
                            color = 'red' if confluence < 0.5 else 'green' if confluence < 0.8 else 'blue'
                            indicator_data.append(html.Span(f" {interval}: {confluence:.2f}", style={'color': color}))

                    elif indicator == 'TPD_DIVERGENCE':
                        if 'TPD_Divergence' in last_row:
                            divergence = last_row.get('TPD_Divergence', 'NO_DIVERGENCE')
                            color = (
                                'red' if 'BEARISH' in divergence else
                                'green' if 'BULLISH' in divergence else
                                'blue' if 'HIDDEN' in divergence else
                                '#888888'
                            )
                            indicator_data.append(html.Span(f" {interval}: {divergence}", style={'color': color}))

                    elif indicator == 'TPD_STRENGTH':
                        if 'TPD_Strength' in last_row:
                            strength = last_row.get('TPD_Strength', 0.0)
                            color = 'red' if strength < 0.6 else 'green' if strength < 0.8 else 'blue'
                            indicator_data.append(html.Span(f" {interval}: {strength:.2f}", style={'color': color}))

                    elif indicator == 'TPD_TREND':
                        if 'TPD_Trend' in last_row:
                            trend = last_row.get('TPD_Trend', 'SIDEWAYS')
                            color = (
                                'green' if trend == 'UPTREND' else
                                'red' if trend == 'DOWNTREND' else
                                'blue' if trend == 'SIDEWAYS' else
                                '#888888'
                            )
                            indicator_data.append(html.Span(f" {interval}: {trend}", style={'color': color}))

                    elif indicator == 'DIV':
                        div_value = last_row.get('DIV', "➖")
                        color = 'gray'
                        if '🟢' in div_value:
                            color = 'green'
                        elif '🔴' in div_value:
                            color = 'red'

                        indicator_data.append(
                            html.Span(f" {interval}: {div_value}", style={'color': color})
                        )
                    elif indicator == 'TRD':
                        trd_label  = last_row.get('COMBINED_TREND_SIGNAL', 'NO_SIGNAL')
                        trd_adx    = last_row.get('TRD_ADX', 0)
                        plus_di    = last_row.get('TRD_PLUS_DI', 0)
                        minus_di   = last_row.get('TRD_MINUS_DI', 0)
                        rsi_val    = last_row.get('TRD_RSI', 0)
                        di_diff    = plus_di - minus_di

                        if trd_label == 'BUY':
                            color      = 'blue' if trd_adx >= 40 else 'green'
                            class_name = ''
                        elif trd_label == 'SELL':
                            color      = 'red'
                            class_name = 'flash' if trd_adx >= 40 else ''
                        elif trd_label == 'WAIT':
                            color      = 'red'
                            class_name = ''
                        else:
                            color      = 'gray'
                            class_name = ''

                        indicator_data.append(
                            html.Span(
                                f" {interval}: A{trd_adx:.0f} D{di_diff:+.0f} R{rsi_val:.0f}",
                                style={'color': color, 'fontWeight': 'bold'},
                                className=class_name
                            )
                        )

                    elif indicator == 'KNS':
                        # KNS artık dashboard'da CANLI hesaplanır — kullanıcının
                        # KNS menüsünden seçtiği HERHANGİ indikatör kombinasyonunu
                        # son-satır kolonlarından oylar (talib yok, ucuz). Menüye
                        # ekleme/çıkarma anında tepki verir.
                        kns = consensus_from_row(last_row, kns_indicators)
                        sig = kns['signal']
                        vb  = int(kns.get('votes_buy', 0))
                        vs  = int(kns.get('votes_sell', 0))
                        vn  = int(kns.get('votes_neutral', 0))

                        if sig == 'STRONG_BUY':
                            color      = 'blue'
                            class_name = ''
                            icon       = '⬆⬆'
                        elif sig == 'BUY':
                            color      = 'green'
                            class_name = ''
                            icon       = '⬆'
                        elif sig == 'STRONG_SELL':
                            color      = 'red'
                            class_name = 'flash'
                            icon       = '⬇⬇'
                        elif sig == 'SELL':
                            color      = 'red'
                            class_name = ''
                            icon       = '⬇'
                        else:
                            color      = 'gray'
                            class_name = ''
                            icon       = '↔'

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {icon} (▲{vb}|▼{vs}|–{vn})",
                                style={'color': color, 'fontWeight': 'bold'},
                                className=class_name,
                                title=f"KNS: {', '.join(kns_indicators) if kns_indicators else 'seçim yok'}"
                            )
                        )

                    elif indicator == 'SR':
                        # SR artık Katman 3'te hesaplanır; df'e SR_SUPPORT/SR_RESISTANCE/
                        # SR_SIGNAL kolonları gömülür. Dashboard SADECE okur.
                        sup = last_row.get('SR_SUPPORT')
                        res = last_row.get('SR_RESISTANCE')
                        sig = last_row.get('SR_SIGNAL')
                        try:
                            sup = float(sup); res = float(res)
                            sr_ok = not (pd.isna(sup) or pd.isna(res))
                        except (TypeError, ValueError):
                            sr_ok = False

                        if not sr_ok or not sig:
                            indicator_data.append(
                                html.Span(f" {interval}: SR yok", style={'color': 'gray'})
                            )
                        else:
                            def _sr_fmt(v):
                                if v >= 1000: return f"{v:.1f}"
                                if v >= 10:   return f"{v:.2f}"
                                if v >= 1:    return f"{v:.4f}"
                                return f"{v:.6f}"

                            if sig == 'support':
                                color, class_name = 'white', ''
                                tip = f"Destekte: {_sr_fmt(sup)}"
                            elif sig == 'resistance':
                                color, class_name = 'white', ''
                                tip = f"Dirençte: {_sr_fmt(res)}"
                            elif sig in ('below_sup', 'near_support'):
                                color, class_name = 'red', ''
                                tip = f"Desteğe yakın: {_sr_fmt(sup)}"
                            elif sig in ('above_res', 'near_resistance'):
                                color, class_name = 'green', ''
                                tip = f"Dirençe yakın: {_sr_fmt(res)}"
                            else:
                                color, class_name = 'gray', ''
                                tip = f"S:{_sr_fmt(sup)}  R:{_sr_fmt(res)}"

                            indicator_data.append(
                                html.Span(
                                    f" {interval}: S{_sr_fmt(sup)} R{_sr_fmt(res)}",
                                    style={'color': color, 'fontWeight': 'bold'},
                                    className=class_name,
                                    title=tip
                                )
                            )

                    elif indicator == 'DIVERGENCE':
                        # Profesyonel divergence görüntüleme
                        div_value = last_row.get('DIVERGENCE', "❓")
                        consensus = last_row.get('DIV_CONSENSUS', 'NEUTRAL')

                        # Renk ve stil belirleme
                        color, class_name = get_divergence_display_style(div_value, consensus)

                        # Tooltip için detay bilgiler
                        reg_bearish = int(last_row.get('DIV_REG_BEARISH', 0))
                        reg_bullish = int(last_row.get('DIV_REG_BULLISH', 0))
                        hid_bearish = int(last_row.get('DIV_HID_BEARISH', 0))
                        hid_bullish = int(last_row.get('DIV_HID_BULLISH', 0))

                        tooltip_text = f"""Regular: {reg_bearish}↓/{reg_bullish}↑
Hidden: {hid_bearish}↓/{hid_bullish}↑
Consensus: {consensus}"""

                        indicator_data.append(
                            html.Span(
                                f" {interval}: {div_value}",
                                style={'color': color, 'cursor': 'help'},
                                className=class_name,
                                title=tooltip_text  # Hover tooltip
                            )
                        )

                    else:
                        indicator_data.append(
                            html.Span(f" {interval}: -", style={'color': 'gray'})
                        )

                # html.Span'leri kompakt dict'e çevir (clientside render edecek)
                _label = indicator_data[0].children
                _cells = []
                for _sp in indicator_data[1:]:
                    _st = getattr(_sp, 'style', None) or {}
                    _txt = _sp.children
                    if not isinstance(_txt, str):
                        _txt = str(_txt)
                    _cells.append({
                        "t": _txt,
                        "c": _st.get('color', ''),
                        "f": getattr(_sp, 'className', None) or '',
                    })
                symbol_data.append({"label": _label, "cells": _cells})
            output.append({"rows": symbol_data})

        # Collector heartbeat — bayatsa/yoksa dashboard "veri akışı durdu" uyarır
        try:
            _hb = redis_cache.client.get('collector:heartbeat')
            _hb = float(_hb) if _hb else None
        except Exception:
            _hb = None
        _stale = (_hb is None) or ((_dt.datetime.now().timestamp() - _hb) > 8)

        return {
            "time": _dt.datetime.now().strftime('%H:%M:%S'),
            "nsym": len(selected_symbols),
            "niv": len(selected_intervals),
            "stale": _stale,
            "groups": output,
        }

    except PreventUpdate:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": f"{type(e).__name__}: {e}"}


@app.callback(
    [Output('symbol-dropdown', 'options'),
     Output('symbol-dropdown', 'value'),
     Output('new-symbol-input', 'value')],
    [Input('add-symbol-button', 'n_clicks')],
    [State('new-symbol-input', 'value'),
     State('symbol-dropdown', 'options'),
     State('symbol-dropdown', 'value')]
)
def add_symbol(n_clicks, new_symbol, current_options, current_values):
    if n_clicks > 0 and new_symbol:
        raw = new_symbol.strip().upper()
        # "USDT" ile bitmiyorsa ekle
        if not raw.endswith("USDT"):
            raw = raw + "USDT"
        new_symbol = raw
        if new_symbol not in [option['value'] for option in current_options]:
            current_options.append({'label': new_symbol, 'value': new_symbol})
            current_values.append(new_symbol)
            global ALL_SYMBOLS, SYMBOL_SOURCE
            if new_symbol not in ALL_SYMBOLS:
                ALL_SYMBOLS.append(new_symbol)
            if new_symbol not in SYMBOL_SOURCE:
                SYMBOL_SOURCE[new_symbol] = "binance"
            # >>> Collector'a sinyal: bu sembolü TOPLAMAYA başla (dinamik ekleme).
            # Producer 'collector:symbols' set'ini izleyip kaynağı çözer + toplar.
            try:
                redis_cache.client.sadd("collector:symbols", new_symbol)
            except Exception as _e:
                print(f"[ADD-SYMBOL] Redis sinyali başarısız: {_e}")
        return current_options, current_values, ''
    return current_options, current_values, new_symbol

@app.callback(
    Output('update-button', 'n_clicks'),
    [Input('update-button', 'n_clicks')],
    [State('symbol-dropdown', 'value'),
     State('interval-dropdown', 'value'),
     State('indicator-dropdown', 'value')]
)
def update_selections(n_clicks, new_symbols, new_intervals, new_indicators):
    if n_clicks > 0:
        global selected_symbols, selected_intervals, selected_indicators
        # Katman 5 (dashboard) veri TOPLAMAZ ve collector BAŞLATMAZ.
        # Sadece kullanıcının filtre seçimini günceller; render callback'i
        # Redis'ten bu seçime göre okur. Collector ayrı process'tir
        # (python3 collector.py) ve evrensel seti zaten hesaplar.
        selected_symbols    = new_symbols
        selected_intervals  = new_intervals
        selected_indicators = new_indicators
        print(f"Selections updated: Symbols={selected_symbols}, "
              f"Intervals={selected_intervals}, Indicators={selected_indicators}")
    return 0
    
async def close_websocket():
    global websocket
    if websocket:
        await websocket.close()


if __name__ == "__main__":
    # Dashboard başlangıç seçimleri — kullanıcı arayüzde değiştirebilir
    syms, ivs, inds = load_setup()
    # Varsayılan: BTC + temel interval'lar (kullanıcı değiştirebilir)
    selected_symbols    = syms if syms else ["BTCUSDT", "ETHUSDT"]
    selected_intervals  = ivs  if ivs  else ["1m", "5m", "15m", "1h"]
    inds = [i for i in (inds or []) if i in VALID_INDICATORS]
    selected_indicators = inds if inds else ["RSI", "RSM", "MFI", "SMI", "CMF", "ADO", "+DI", "CCI", "WIL"]

    # NOT: Dashboard kendi başına veri çekmez.
    # Collector çalışıyorsa Redis'te veri var — dashboard onu okur.
    # Collector çalışmıyorsa cache boş görünür.
    print(f"[DASHBOARD] Starting — http://0.0.0.0:9050")
    print(f"[DASHBOARD] Collector must run in a separate terminal: python3 collector.py 4")
    app.run(host='0.0.0.0', port=9050, debug=False, use_reloader=False)