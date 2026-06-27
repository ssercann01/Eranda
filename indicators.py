"""
ERANDA V2 — indicators.py  (Katman 3 — hesap kütüphanesi)
================================================================
TÜM CPU-ağır hesaplama burada. SADECE consumer process import eder.
Producer (WS/REST) bu dosyayı ASLA import etmez → event loop'ta talib yok.

ÖNEMLİ: Aşağıdaki fonksiyon/sınıfların GÖVDESİ collector.py'den BİREBİR
taşındı. Tek bir satır iş mantığı değiştirilmedi. Sadece bulundukları
dosya değişti (sorumluluk ayrımı).

Taşınan üst-düzey isimler:
  parallel_calculate_indicators, hesapla_rsm, calculate_smi,
  detect_professional_divergence_per_interval, ProfessionalTPDIndicator,
  ProfessionalDivergenceDetector, format_professional_divergence_output,
  get_divergence_display_style, setup_divergence_cache_optimization,
  calculate_trp, calculate_support_resistance, get_sr_signal,
  optimize_dataframe, validate_trend_quality, calculate_trend_strength,
  filter_signals, backtest_trend_indicator, safe_indicator_calculation,
  generate_trend_report, calculate_indicators_async,
  calculate_fibonacci_retracement, _indicator_vote, calculate_consensus_signal,
  calculate_indicators, calculate_indicators_in_parallel, update_indicators,
  update_column  (+ modül sabitleri _sr_cache, INDICATOR_WEIGHTS)
"""
from shared import *

# ── Consumer-process'e ait state (eski collector global'leri) ───────────────
# Bu cache yalnızca consumer process içinde yaşar. Tek truth source Redis'tir;
# bu in-memory cache sadece "son df"yi tutup incremental update için kullanılır.
#
# RAM DÜZELTMESİ: shared'daki CACHE_SIZE=500 idi — her key için 500 DataFrame
# tutuyordu, oysa kod yalnızca cache[key][-1]'i okur. 169 key × 500 df = GB'larca
# boşa RAM. Sadece son birkaç df yeterli → 3.
CACHE_SIZE = 3
cache      = defaultdict(lambda: deque(maxlen=CACHE_SIZE))
cache_lock = threading.Lock()

# TPD incremental pencere — collector.py'den taşındı (calc bloğu kullanır)
_TPD_WINDOW: dict = {
    '1m': 120, '3m': 120, '5m': 120,
    '15m': 150, '30m': 150, '1h': 150,
    '2h': 200, '4h': 200, '6h': 200,
    '8h': 200, '12h': 250, '1d': 300, '3d': 300,
}

# ═══════════════════════════════════════════════════════════════════════════
#  AŞAĞISI collector.py satır 289–3157 ARASINDAN BİREBİR KOPYALANDI
# ═══════════════════════════════════════════════════════════════════════════

def parallel_calculate_indicators(dataframes, selected_indicators, interval, symbol):
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(calculate_indicators, df, selected_indicators, interval, symbol)
            for df in dataframes
        ]
        return [f.result() for f in futures]

def hesapla_rsm(df, periyot=14):
    kapanış_fiyatları = df['close']
    # Fiyat değişimlerini hesapla
    fiyat_değişimi = kapanış_fiyatları.diff()
    # Kazanç ve kayıpları hesapla
    kazanç = fiyat_değişimi.where(fiyat_değişimi > 0, 0)
    kayıp = -fiyat_değişimi.where(fiyat_değişimi < 0, 0)
    # Ortalama kazanç ve kayıp
    ortalama_kazanç = kazanç.rolling(window=periyot).mean()
    ortalama_kayıp = kayıp.rolling(window=periyot).mean()
    # RS hesapla
    rs = ortalama_kazanç / ortalama_kayıp.replace(0, 1)
    # RSM hesapla
    rsm = 100 - (100 / (1 + rs))
    df['RSM'] = rsm.fillna(0)
    return df

def calculate_smi(data, period=14, smoothing1=3, smoothing2=3):
    high, low, close = data['high'], data['low'], data['close']
    hl2 = (high + low) / 2
    ll = low.rolling(window=period).min()
    hh = high.rolling(window=period).max()
    diff = hh - ll
    rdiff = close - (hh + ll) / 2
    avg_diff = rdiff.ewm(span=smoothing1).mean()
    avg_diff_smooth = avg_diff.ewm(span=smoothing2).mean()
    avg_denom = diff.ewm(span=smoothing1).mean()
    avg_denom_smooth = avg_denom.ewm(span=smoothing2).mean()
    smi = 100 * (avg_diff_smooth / (avg_denom_smooth / 2)).fillna(0)
    return smi



def detect_professional_divergence_per_interval(df, interval):
    try:
        signals = []
        score = 0  # Basit güven skoru, pozitif divergence pozitif puan

        close = df['close']
        # Helper function to check divergence pattern
        def check_divergence(indicator):
            if df[indicator].iloc[-1] > df[indicator].iloc[-2] and close.iloc[-1] < close.iloc[-2]:
                return 'bearish'  # Negatif divergence
            elif df[indicator].iloc[-1] < df[indicator].iloc[-2] and close.iloc[-1] > close.iloc[-2]:
                return 'bullish'  # Pozitif divergence
            return None

        # Listeye ekle - (gösterge, kısa isim)
        indicators = [
            ('MFI', 'M'),
            ('RSI', 'R'),
            ('MCD', 'M'),
            ('Net_Volume', 'V'),
            ('CCI', 'C'),
            ('CMF', 'C'),
            ('WIL', 'W'),
            ('+DI', 'D'),
            ('EMA_diff', 'E')  # EMA farkı sütunu olmalı (ör. EMA50 - EMA200)
        ]

        # Eğer EMA_diff yoksa hesapla (EMA50 - EMA200)
        if 'EMA_diff' not in df.columns:
            if 'EMA50' in df.columns and 'EMA200' in df.columns:
                df['EMA_diff'] = df['EMA50'] - df['EMA200']
            else:
                indicators = [i for i in indicators if i[0] != 'EMA_diff']

        for col, name in indicators:
            if col not in df.columns:
                continue
            div_type = check_divergence(col)
            if div_type == 'bearish':
                signals.append(f"🔴{name}")
                score -= 1
            elif div_type == 'bullish':
                signals.append(f"🟢{name}")
                score += 1

        if not signals:
            return {interval: "➖"}

        confidence = "H" if abs(score) >= 3 else "M" if abs(score) == 2 else "L"
        signal_str = "|".join(signals) + f"({confidence})"
        return {interval: signal_str}
    
    except Exception as e:
        return {interval: f"Hata: {str(e)}"}



# Duplicate import bloğu kaldırıldı — tüm import'lar dosya başında mevcut
# scipy.stats hâlâ kullanılmıyor (linregress polyfit ile değiştirildi), numba da kaldırıldı
warnings.filterwarnings('ignore')

class ProfessionalTPDIndicator:
    def __init__(self):
        self.base_weights = {
            'rsi': 0.25,
            'mfi': 0.25,
            'smi': 0.25,
            'cci': 0.15,
            'wil': 0.10
        }
        
        # Adaptif parametreler
        self.volatility_lookback = 20
        self.trend_confirmation_periods = 3
        self.reliability_threshold = 0.5
        self.signal_consistency_periods = 2
        
        # Volatilite bazlı sinyal eşikleri (temel değerler)
        self.base_signal_thresholds = {
            'strong_buy': 45,
            'buy': 15,
            'strong_sell': -45,
            'sell': -15
        }
        
        # Risk faktörleri
        self.risk_factors = {
            'volatility_weight': 0.4,
            'volume_weight': 0.3,
            'momentum_weight': 0.3
        }
        
        # Zaman dilimi bazlı onay periyodları
        self.signal_confirmation_periods = {
            '1m': 3, '3m': 3, '5m': 3, '15m': 2, '30m': 2, 
            '1h': 2, '2h': 2, '4h': 2, '6h': 2, '8h': 2, '12h': 2, '1d': 2
        }

    def calculate_advanced_tpd(self, df: pd.DataFrame, interval: str = '1m') -> pd.DataFrame:
        """Ana hesaplama fonksiyonu - vektörleştirilmiş ve optimize edilmiş"""
        # Veri doğrulama
        if not self._validate_input_data(df):
            raise ValueError("Geçersiz veri çerçevesi")

        try:
            # Incremental window — full DataFrame yerine son N satır hesapla
            # Rolling indikatörler için min 100 satır yeterli, fazlası boşa CPU
            win = _TPD_WINDOW.get(interval, 200)
            compute_df = df.tail(win).copy() if len(df) > win else df.copy()
            
            # 1. Temel göstergeleri hesapla (vektörleştirilmiş)
            indicators = self._calculate_base_indicators_vectorized(compute_df)

            # 2. Volatilite ve risk metriklerini hesapla
            volatility_metrics = self._calculate_volatility_metrics(compute_df)

            # 3. Normalizasyon (bilimsel yaklaşım)
            normalized_indicators = self._normalize_indicators_scientific(indicators)

            # 4. Adaptif ağırlıklar
            dynamic_weights = self._calculate_adaptive_weights(volatility_metrics, compute_df)

            # 5. Composite TPD skoru
            compute_df['TPD'] = self._calculate_composite_score_vectorized(
                normalized_indicators, dynamic_weights, volatility_metrics
            )

            # 6. Destekleyici metrikler
            compute_df['TPD_Reliability'] = self._calculate_reliability_score_advanced(
                compute_df, indicators, volatility_metrics
            )
            compute_df['TPD_Momentum'] = self._calculate_momentum_vectorized(compute_df['TPD'])
            compute_df['TPD_Volatility_Factor'] = volatility_metrics['volatility_factor']

            # 7. Adaptif sinyal eşikleri
            adaptive_thresholds = self._calculate_adaptive_thresholds(volatility_metrics)

            # 8. Sinyal üretimi (vektörleştirilmiş)
            compute_df['TPD_Raw_Signal'] = self._generate_signals_vectorized(
                compute_df['TPD'], compute_df['TPD_Reliability'],
                compute_df['TPD_Momentum'], adaptive_thresholds
            )

            # 9. Diverjans analizi
            compute_df['TPD_Divergence'] = self._calculate_divergence_vectorized(compute_df)

            # 10. Sinyal onayı
            compute_df['TPD_Signal'] = self._confirm_signals_vectorized(
                compute_df['TPD_Raw_Signal'], compute_df['TPD_Divergence'],
                compute_df['TPD_Momentum'], interval
            )

            # 11. Ek analizler
            compute_df['TPD_Trend']           = self._calculate_trend_direction_vectorized(compute_df['TPD'])
            compute_df['TPD_Risk']            = self._calculate_comprehensive_risk(compute_df, volatility_metrics)
            compute_df['TPD_MTF_Confluence']  = self._calculate_mtf_confluence_advanced(compute_df)
            compute_df['TPD_Strength']        = self._calculate_signal_strength(compute_df, volatility_metrics)

            # Final validasyon
            self._validate_output(compute_df)

            # Sadece hesaplanan TPD sütunlarını orijinal df'e son satır üzerinden aktar
            tpd_cols = ['TPD', 'TPD_Reliability', 'TPD_Momentum', 'TPD_Volatility_Factor',
                        'TPD_Raw_Signal', 'TPD_Divergence', 'TPD_Signal',
                        'TPD_Trend', 'TPD_Risk', 'TPD_MTF_Confluence', 'TPD_Strength']
            last_idx = compute_df.index[-1]
            for col in tpd_cols:
                if col in compute_df.columns:
                    df.loc[last_idx, col] = compute_df.at[last_idx, col]

            return df
            
        except Exception as e:
            raise RuntimeError(f"TPD hesaplama hatası: {str(e)}")

    def _validate_input_data(self, df: pd.DataFrame) -> bool:
        """Gelişmiş veri doğrulama"""
        required_columns = {'open', 'high', 'low', 'close', 'volume'}
        
        if df.empty or len(df) < 50:
            return False
            
        if not required_columns.issubset(df.columns):
            return False
            
        # Veri kalitesi kontrolü
        if df[['high', 'low', 'close', 'volume']].isnull().sum().sum() > len(df) * 0.1:
            return False
            
        return True

    def _calculate_base_indicators_vectorized(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """Vektörleştirilmiş gösterge hesaplamaları"""
        # Temel değerler
        high, low, close, volume = df['high'], df['low'], df['close'], df['volume']
        
        indicators = {
            'rsi': pd.Series(talib.RSI(close, timeperiod=14), index=df.index),
            'mfi': pd.Series(talib.MFI(high, low, close, volume, timeperiod=14), index=df.index),
            'smi': self._calculate_smi_vectorized(df),
            'cci': pd.Series(talib.CCI(high, low, close, timeperiod=14), index=df.index),
            'wil': pd.Series(talib.WILLR(high, low, close, timeperiod=14), index=df.index),
            'adx': pd.Series(talib.ADX(high, low, close, timeperiod=14), index=df.index)
        }
        
        # NaN değerleri düzelt — bfill() kaldırıldı (geriye dönük veri sızıntısı önlenir)
        for key in indicators:
            indicators[key] = indicators[key].ffill()
        
        return indicators

    def _calculate_volatility_metrics(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """Kapsamlı volatilite metrikleri"""
        returns = df['close'].pct_change()
        
        # Çoklu volatilite ölçümleri
        rolling_std = returns.rolling(window=self.volatility_lookback).std()
        rolling_var = returns.rolling(window=self.volatility_lookback).var()
        
        # Parkinson volatilitesi (high-low bazlı)
        parkinson_vol = np.sqrt(
            (np.log(df['high'] / df['low']) ** 2).rolling(window=self.volatility_lookback).mean()
        ) * np.sqrt(252)
        
        # Garman-Klass volatilitesi
        gk_vol = np.sqrt(
            (0.5 * (np.log(df['high'] / df['low']) ** 2) - 
             (2 * np.log(2) - 1) * (np.log(df['close'] / df['open']) ** 2))
            .rolling(window=self.volatility_lookback).mean()
        ) * np.sqrt(252)
        
        # rolling_std'yi de normalize et — ham değer yerine kendi 50-periyot ortalamasına böl
        # Böylece üç bileşen aynı ölçekte (ratio ~1.0 etrafında) karşılaştırılabilir olur
        rolling_std_norm = rolling_std / rolling_std.rolling(50).mean().replace(0, np.nan)

        # Kompozit volatilite faktörü — tüm bileşenler normalize edilmiş ratio
        volatility_factor = (rolling_std_norm.fillna(1.0) * 0.4 +
                           (parkinson_vol / parkinson_vol.rolling(50).mean().replace(0, np.nan)).fillna(1.0) * 0.3 +
                           (gk_vol / gk_vol.rolling(50).mean().replace(0, np.nan)).fillna(1.0) * 0.3)
        
        return {
            'returns': returns,
            'rolling_std': rolling_std,
            'parkinson_vol': parkinson_vol,
            'gk_vol': gk_vol,
            'volatility_factor': volatility_factor.fillna(1.0)
        }

    def _normalize_indicators_scientific(self, indicators: Dict[str, pd.Series]) -> Dict[str, pd.Series]:
        """Bilimsel normalizasyon yaklaşımı"""
        normalized = {}
        
        # RSI ve MFI: 0-100 -> -100 ile +100 (standart)
        normalized['rsi'] = (indicators['rsi'] - 50) * 2
        normalized['mfi'] = (indicators['mfi'] - 50) * 2
        
        # SMI: Zaten -100 ile +100 arasında
        normalized['smi'] = np.clip(indicators['smi'], -100, 100)
        
        # CCI: Z-score normalizasyonu
        cci_mean = indicators['cci'].rolling(window=100).mean()
        cci_std = indicators['cci'].rolling(window=100).std()
        normalized['cci'] = ((indicators['cci'] - cci_mean) / cci_std).fillna(0) * 20
        normalized['cci'] = np.clip(normalized['cci'], -100, 100)
        
        # Williams %R: -100,0 -> -100,+100
        normalized['wil'] = indicators['wil'] * 2 + 100
        
        # ADX: Trend gücü çarpanı (0-1)
        normalized['adx_strength'] = np.clip(indicators['adx'] / 50, 0, 1)
        
        return normalized

    def _calculate_adaptive_weights(self, volatility_metrics: Dict[str, pd.Series], 
                                  df: pd.DataFrame) -> pd.DataFrame:
        """Adaptif ağırlık hesaplama"""
        vol_factor = volatility_metrics['volatility_factor']
        volume_factor = df['volume'] / df['volume'].rolling(window=20).mean()
        
        # Temel ağırlıklar
        weights_df = pd.DataFrame(index=df.index)
        for key, base_weight in self.base_weights.items():
            weights_df[key] = base_weight
        
        # Volatilite adaptasyonu
        high_vol_mask = vol_factor > vol_factor.quantile(0.7)
        weights_df.loc[high_vol_mask, 'rsi'] *= 1.2
        weights_df.loc[high_vol_mask, 'cci'] *= 1.3
        weights_df.loc[high_vol_mask, 'smi'] *= 1.1
        
        # Hacim adaptasyonu
        high_volume_mask = volume_factor > 1.5
        weights_df.loc[high_volume_mask, 'mfi'] *= 1.15
        weights_df.loc[high_volume_mask, 'rsi'] *= 1.1
        
        # Ağırlıkları normalize et
        weights_sum = weights_df.sum(axis=1)
        for col in weights_df.columns:
            weights_df[col] = weights_df[col] / weights_sum
            
        return weights_df.ffill()

    def _calculate_composite_score_vectorized(self, indicators: Dict[str, pd.Series], 
                                            weights: pd.DataFrame, 
                                            volatility_metrics: Dict[str, pd.Series]) -> pd.Series:
        """Vektörleştirilmiş composite score hesaplaması"""
        # Ana göstergeleri birleştir
        indicator_keys = ['rsi', 'mfi', 'smi', 'cci', 'wil']
        
        composite_score = pd.Series(0.0, index=indicators['rsi'].index)
        
        for key in indicator_keys:
            composite_score += indicators[key] * weights[key]
        
        # ADX trend gücü çarpanı — normalized dict'ten al (indicators'da değil, normalized'da)
        adx_multiplier = 0.5 + (indicators['adx_strength'] * 0.5)
        composite_score *= adx_multiplier
        
        # Volatilite düzeltmesi
        vol_adjustment = 1.0 + (volatility_metrics['volatility_factor'] - 1.0) * 0.3
        composite_score *= vol_adjustment
        
        return composite_score.fillna(0)

    def _calculate_reliability_score_advanced(self, df: pd.DataFrame, 
                                            indicators: Dict[str, pd.Series],
                                            volatility_metrics: Dict[str, pd.Series]) -> pd.Series:
        """Gelişmiş güvenilirlik skoru"""
        # ADX güvenilirliği
        adx_reliability = np.where(indicators['adx'] > 25, 1.0, 
                                 np.where(indicators['adx'] > 15, 0.8, 0.6))
        
        # Hacim güvenilirliği
        volume_sma = df['volume'].rolling(window=20).mean()
        volume_reliability = np.where(df['volume'] > volume_sma * 0.8, 1.0, 0.7)
        
        # Volatilite güvenilirliği — yüksek volatilite güvenilirliği DÜŞÜRÜR (düzeltildi)
        # Eski: vol_factor > 1.2 → 1.0 (yüksek vol = güvenilir — YANLIŞ)
        # Yeni: vol_factor > 1.2 → 0.7 (yüksek vol = daha az güvenilir — DOĞRU)
        vol_factor = volatility_metrics['volatility_factor']
        vol_reliability = np.where(vol_factor > 1.2, 0.7,
                                 np.where(vol_factor > 0.8, 0.9, 1.0))
        
        # Gösterge uyumu
        indicator_agreement = self._calculate_indicator_agreement(indicators)
        
        # Kompozit güvenilirlik
        reliability = (adx_reliability * 0.3 + 
                      volume_reliability * 0.3 + 
                      vol_reliability * 0.2 + 
                      indicator_agreement * 0.2)
        
        return pd.Series(reliability, index=df.index).fillna(0.5)

    def _calculate_indicator_agreement(self, indicators: Dict[str, pd.Series]) -> np.ndarray:
        """Gösterge uyumu hesaplama"""
        # Normalleştirilmiş göstergelerin yönlerini karşılaştır
        rsi_signal = np.where(indicators['rsi'] > 50, 1, -1)
        mfi_signal = np.where(indicators['mfi'] > 50, 1, -1)
        smi_signal = np.where(indicators['smi'] > 0, 1, -1)
        cci_signal = np.where(indicators['cci'] > 0, 1, -1)
        wil_signal = np.where(indicators['wil'] > -50, 1, -1)
        
        # Uyum yüzdesi
        signals = np.array([rsi_signal, mfi_signal, smi_signal, cci_signal, wil_signal])
        agreement = np.abs(signals.sum(axis=0)) / 5.0
        
        return agreement

    def _calculate_momentum_vectorized(self, tpd: pd.Series) -> pd.Series:
        """Vektörleştirilmiş momentum hesaplama"""
        # Çoklu periyot momentum
        momentum_3 = tpd.diff(3)
        momentum_5 = tpd.diff(5)
        momentum_10 = tpd.diff(10)
        
        # Ağırlıklı momentum
        weighted_momentum = (momentum_3 * 0.5 + 
                           momentum_5 * 0.3 + 
                           momentum_10 * 0.2)
        
        return weighted_momentum.fillna(0)

    def _calculate_adaptive_thresholds(self, volatility_metrics: Dict[str, pd.Series]) -> pd.DataFrame:
        """Adaptif sinyal eşikleri"""
        vol_factor = volatility_metrics['volatility_factor']
        
        # Volatilite bazlı eşik ayarlama
        threshold_multiplier = 1.0 + (vol_factor - 1.0) * 0.5
        threshold_multiplier = np.clip(threshold_multiplier, 0.5, 1.5)
        
        thresholds = pd.DataFrame(index=vol_factor.index)
        for key, base_value in self.base_signal_thresholds.items():
            thresholds[key] = base_value * threshold_multiplier
        
        return thresholds.ffill()

    def _generate_signals_vectorized(self, tpd: pd.Series, reliability: pd.Series, 
                                   momentum: pd.Series, thresholds: pd.DataFrame) -> pd.Series:
        """Vektörleştirilmiş sinyal üretimi"""
        signals = pd.Series('NEUTRAL', index=tpd.index)
        
        # Güvenilirlik maskeleri
        high_reliability = reliability > self.reliability_threshold
        medium_reliability = reliability > 0.3
        
        # Momentum koşulları
        strong_pos_momentum = momentum > 2.0
        strong_neg_momentum = momentum < -2.0
        weak_pos_momentum = momentum > -1.0
        weak_neg_momentum = momentum < 1.0
        
        # Sinyal koşulları
        strong_buy_mask = (tpd > thresholds['strong_buy']) & high_reliability & strong_pos_momentum
        strong_sell_mask = (tpd < thresholds['strong_sell']) & high_reliability & strong_neg_momentum
        
        buy_mask = ((tpd > thresholds['buy']) & 
                   (tpd <= thresholds['strong_buy']) & 
                   high_reliability & weak_pos_momentum)
        
        sell_mask = ((tpd < thresholds['sell']) & 
                    (tpd >= thresholds['strong_sell']) & 
                    high_reliability & weak_neg_momentum)
        
        weak_buy_mask = ((tpd > thresholds['buy']) & 
                        medium_reliability & ~high_reliability & 
                        (momentum > -2.0))
        
        weak_sell_mask = ((tpd < thresholds['sell']) & 
                         medium_reliability & ~high_reliability & 
                         (momentum < 2.0))
        
        # Momentum reversal — yeniden tasarlandı
        # BUY reversal: TPD negatif bölgede ancak momentum yukarı döndü (dip reversal)
        # SELL reversal: TPD pozitif bölgede ancak momentum aşağı döndü (tepe reversal)
        # Eski: (tpd > 0) & (momentum < -3) → tepede devam sinyali değil düşüş demek, BUY olamaz
        momentum_reversal_buy_mask  = (tpd < 0) & (momentum > 3.0) & high_reliability & (tpd > tpd.shift(1))
        momentum_reversal_sell_mask = (tpd > 0) & (momentum < -3.0) & high_reliability & (tpd < tpd.shift(1))
        
        # Sinyal atama — zayıftan güçlüye doğru yaz (güçlü sinyaller üstteki zayıfı ezer)
        # MOMENTUM_REVERSAL yalnızca NEUTRAL kalan yerlere yazılır — güçlü sinyallerin üzerine geçmez
        signals.loc[weak_buy_mask] = 'WEAK_BUY'
        signals.loc[weak_sell_mask] = 'WEAK_SELL'
        signals.loc[momentum_reversal_buy_mask & (signals == 'NEUTRAL')] = 'MOMENTUM_REVERSAL_BUY'
        signals.loc[momentum_reversal_sell_mask & (signals == 'NEUTRAL')] = 'MOMENTUM_REVERSAL_SELL'
        signals.loc[buy_mask] = 'BUY'
        signals.loc[sell_mask] = 'SELL'
        signals.loc[strong_buy_mask] = 'STRONG_BUY'
        signals.loc[strong_sell_mask] = 'STRONG_SELL'
        
        return signals

    def _calculate_divergence_vectorized(self, df: pd.DataFrame) -> pd.Series:
        """Vektörleştirilmiş diverjans hesaplama"""
        divergence = pd.Series('NO_DIVERGENCE', index=df.index)
        
        # linregress() → np.polyfit(deg=1) ile değiştirildi: rolling apply içinde 3-5x daha hızlı
        lookback = 10
        _x = np.arange(lookback, dtype=np.float64)

        price_slope = df['close'].rolling(window=lookback).apply(
            lambda y: np.polyfit(_x, y, 1)[0], raw=True
        )

        tpd_slope = df['TPD'].rolling(window=lookback).apply(
            lambda y: np.polyfit(_x, y, 1)[0], raw=True
        )
        
        # Diverjans koşulları
        bearish_div = (price_slope > 0.001) & (tpd_slope < -0.001)
        bullish_div = (price_slope < -0.001) & (tpd_slope > 0.001)
        weak_div = (np.abs(price_slope - tpd_slope) > 0.002) & ~bearish_div & ~bullish_div
        
        divergence.loc[bearish_div] = 'BEARISH_DIVERGENCE'
        divergence.loc[bullish_div] = 'BULLISH_DIVERGENCE'
        divergence.loc[weak_div] = 'WEAK_DIVERGENCE'
        
        return divergence

    def _confirm_signals_vectorized(self, raw_signals: pd.Series, divergence: pd.Series,
                                  momentum: pd.Series, interval: str) -> pd.Series:
        """Sinyal onayı — Python for loop kaldırıldı, tam vektörleştirildi"""
        confirmation_periods = self.signal_confirmation_periods.get(interval, 2)

        # Momentum tutarlılığı: rolling window ortalama ve trend
        mom_avg   = momentum.rolling(window=confirmation_periods + 1, min_periods=1).mean()
        mom_trend = momentum - momentum.shift(confirmation_periods)

        # BUY tutarlılığı: son N barda hiç SELL/STRONG_SELL yoksa tutarlı
        is_buy_signal  = raw_signals.isin(['STRONG_BUY', 'BUY', 'WEAK_BUY', 'MOMENTUM_REVERSAL_BUY'])
        is_sell_signal = raw_signals.isin(['STRONG_SELL', 'SELL', 'WEAK_SELL', 'MOMENTUM_REVERSAL_SELL'])

        # Rolling any() — son N barda ters yönlü sinyal var mı?
        sell_in_window = is_sell_signal.rolling(window=confirmation_periods + 1, min_periods=1).max().astype(bool)
        buy_in_window  = is_buy_signal.rolling(window=confirmation_periods + 1, min_periods=1).max().astype(bool)

        # Momentum tutarlılık maskeleri
        buy_momentum_ok  = (mom_avg > -2.0) & (mom_trend > -1.0)
        sell_momentum_ok = (mom_avg <  2.0) & (mom_trend <  1.0)

        # Diverjans maskeleri
        bearish_div = divergence == 'BEARISH_DIVERGENCE'
        bullish_div = divergence == 'BULLISH_DIVERGENCE'
        weak_div    = divergence == 'WEAK_DIVERGENCE'

        confirmed_signals = raw_signals.copy()

        # BUY sinyalleri → momentum veya diverjans uyumsuzsa düşür
        buy_mask = raw_signals.isin(['STRONG_BUY', 'BUY'])
        buy_fail = buy_mask & (sell_in_window | ~buy_momentum_ok | bearish_div)
        confirmed_signals.loc[buy_fail] = 'WEAK_BUY'

        # STRONG sinyaller weak_div ile de düşürülür
        strong_buy_fail = (raw_signals == 'STRONG_BUY') & weak_div
        confirmed_signals.loc[strong_buy_fail] = 'WEAK_BUY'

        sell_mask = raw_signals.isin(['STRONG_SELL', 'SELL'])
        sell_fail = sell_mask & (buy_in_window | ~sell_momentum_ok | bullish_div)
        confirmed_signals.loc[sell_fail] = 'WEAK_SELL'

        strong_sell_fail = (raw_signals == 'STRONG_SELL') & weak_div
        confirmed_signals.loc[strong_sell_fail] = 'WEAK_SELL'

        return confirmed_signals

    def _check_signal_consistency_vectorized(self, recent_signals: pd.Series, 
                                           current_signal: str) -> bool:
        """Vektörleştirilmiş sinyal tutarlılığı"""
        if current_signal in ['STRONG_BUY', 'STRONG_SELL']:
            return (recent_signals == current_signal).all()
        
        if current_signal in ['BUY', 'WEAK_BUY']:
            buy_signals = ['BUY', 'WEAK_BUY', 'STRONG_BUY']
            return recent_signals.isin(buy_signals + ['NEUTRAL']).all()
        
        if current_signal in ['SELL', 'WEAK_SELL']:
            sell_signals = ['SELL', 'WEAK_SELL', 'STRONG_SELL']
            return recent_signals.isin(sell_signals + ['NEUTRAL']).all()
        
        return True

    def _check_momentum_consistency_vectorized(self, recent_momentum: pd.Series, 
                                             current_signal: str) -> bool:
        """Vektörleştirilmiş momentum tutarlılığı"""
        if len(recent_momentum) < 2:
            return False
            
        avg_momentum = recent_momentum.mean()
        momentum_trend = recent_momentum.iloc[-1] - recent_momentum.iloc[0]
        
        if current_signal in ['STRONG_BUY', 'BUY']:
            return avg_momentum > -2.0 and momentum_trend > -1.0
        
        if current_signal in ['STRONG_SELL', 'SELL']:
            return avg_momentum < 2.0 and momentum_trend < 1.0
        
        return True

    def _check_divergence_impact_vectorized(self, current_divergence: str, 
                                          current_signal: str) -> bool:
        """Vektörleştirilmiş diverjans etkisi"""
        if current_divergence == 'BEARISH_DIVERGENCE' and current_signal in ['BUY', 'STRONG_BUY']:
            return False
        
        if current_divergence == 'BULLISH_DIVERGENCE' and current_signal in ['SELL', 'STRONG_SELL']:
            return False
        
        if current_divergence == 'WEAK_DIVERGENCE' and current_signal in ['STRONG_BUY', 'STRONG_SELL']:
            return False
        
        return True

    def _calculate_trend_direction_vectorized(self, tpd: pd.Series) -> pd.Series:
        """Vektörleştirilmiş trend yönü"""
        trend = pd.Series('SIDEWAYS', index=tpd.index)
        
        # Rolling trend analizi — linregress yerine polyfit (performans)
        _x = np.arange(self.trend_confirmation_periods, dtype=np.float64)
        rolling_slope = tpd.rolling(window=self.trend_confirmation_periods).apply(
            lambda y: np.polyfit(_x, y, 1)[0], raw=True
        )
        
        uptrend_mask = rolling_slope > 0.5
        downtrend_mask = rolling_slope < -0.5
        
        trend.loc[uptrend_mask] = 'UPTREND'
        trend.loc[downtrend_mask] = 'DOWNTREND'
        
        return trend

    def _calculate_comprehensive_risk(self, df: pd.DataFrame, 
                                    volatility_metrics: Dict[str, pd.Series]) -> pd.Series:
        """Kapsamlı risk analizi"""
        # Volatilite riski
        vol_factor = volatility_metrics['volatility_factor']
        vol_risk = np.clip((vol_factor - 1.0) * 2, 0, 1)
        
        # Hacim riski — düşük hacim (volume < SMA) riskli, yüksek hacim güvenilirlik sağlar
        # Eski: np.clip((volume/sma - 1), 0, 1)  →  yüksek hacimi riskli sayıyordu (YANLIŞ)
        # Yeni: np.clip((1 - volume/sma), 0, 1)  →  düşük hacimi riskli sayar (DOĞRU)
        volume_sma = df['volume'].rolling(window=20).mean().replace(0, np.nan)
        volume_risk = np.clip((1.0 - df['volume'] / volume_sma), 0, 1).fillna(0.5)
        
        # Momentum riski
        momentum_risk = np.clip(np.abs(df['TPD_Momentum']) / 10.0, 0, 1)
        
        # Kompozit risk skoru
        risk_score = (vol_risk * self.risk_factors['volatility_weight'] +
                     volume_risk * self.risk_factors['volume_weight'] +
                     momentum_risk * self.risk_factors['momentum_weight'])
        
        # Risk kategorileri
        risk_categories = pd.Series('MEDIUM', index=df.index)
        risk_categories.loc[risk_score > 0.7] = 'HIGH'
        risk_categories.loc[risk_score < 0.3] = 'LOW'
        
        return risk_categories

    def _calculate_mtf_confluence_advanced(self, df: pd.DataFrame) -> pd.Series:
        """Gelişmiş çok zaman dilimi confluence"""
        # Çoklu SMA'lar
        sma_5 = df['TPD'].rolling(window=5).mean()
        sma_10 = df['TPD'].rolling(window=10).mean()
        sma_20 = df['TPD'].rolling(window=20).mean()
        
        # Confluence skorları
        short_term_alignment = ((sma_5 > 0) == (sma_10 > 0)).astype(int)
        medium_term_alignment = ((sma_10 > 0) == (sma_20 > 0)).astype(int)
        trend_strength = np.abs(sma_5 - sma_20) / 50.0
        
        confluence_score = (short_term_alignment * 0.4 + 
                           medium_term_alignment * 0.4 + 
                           np.clip(trend_strength, 0, 1) * 0.2)
        
        return confluence_score

    def _calculate_signal_strength(self, df: pd.DataFrame, 
                                 volatility_metrics: Dict[str, pd.Series]) -> pd.Series:
        """Sinyal gücü hesaplama"""
        # TPD mutlak değeri
        tpd_strength = np.abs(df['TPD']) / 100.0
        
        # Güvenilirlik desteği
        reliability_boost = df['TPD_Reliability']
        
        # Volatilite etkisi — yüksek volatilite sinyal gücünü AZALTIR (düzeltildi)
        # Eski: clip(vol_factor, 0.5, 1.5) → yüksek vol çarpanını artırıyordu
        # Yeni: vol_factor büyüdükçe çarpan küçülür (1/vol_factor mantığı)
        vol_boost = np.clip(1.0 / volatility_metrics['volatility_factor'].replace(0, np.nan).fillna(1.0), 0.5, 1.2)
        
        # Momentum desteği
        momentum_boost = np.clip(np.abs(df['TPD_Momentum']) / 5.0, 0, 1)
        
        # Kompozit güç
        signal_strength = (tpd_strength * reliability_boost * vol_boost * 
                          (1 + momentum_boost * 0.3))
        
        return np.clip(signal_strength, 0, 1)

    def _calculate_smi_vectorized(self, df: pd.DataFrame, k_period: int = 14, 
                                d_period: int = 3) -> pd.Series:
        """Vektörleştirilmiş SMI hesaplama"""
        highest_high = df['high'].rolling(window=k_period).max()
        lowest_low = df['low'].rolling(window=k_period).min()
        
        close_position = df['close'] - (highest_high + lowest_low) / 2
        high_low_range = (highest_high - lowest_low) / 2
        
        # Sıfır bölme kontrolü
        high_low_range = high_low_range.replace(0, np.nan)
        
        # SMI hesaplama
        smi_raw = (close_position.rolling(window=d_period).mean() / 
                   high_low_range.rolling(window=d_period).mean())
        
        return (smi_raw * 100).fillna(0)

    def _validate_output(self, df: pd.DataFrame) -> None:
        """Çıkış verilerini doğrula"""
        required_columns = [
            'TPD', 'TPD_Reliability', 'TPD_Momentum', 'TPD_Volatility_Factor',
            'TPD_Raw_Signal', 'TPD_Divergence', 'TPD_Signal', 'TPD_Trend',
            'TPD_Risk', 'TPD_MTF_Confluence', 'TPD_Strength'
        ]
        
        for col in required_columns:
            if col not in df.columns:
                raise ValueError(f"Eksik çıkış kolonu: {col}")
            
            if df[col].isnull().sum() > len(df) * 0.5:
                raise ValueError(f"Çok fazla NaN değer: {col}")

    def get_signal_analysis(self, df: pd.DataFrame) -> Dict:
        """Sinyal analizi özeti"""
        if 'TPD_Signal' not in df.columns:
            raise ValueError("TPD analizi önce hesaplanmalı")
        
        latest_data = df.iloc[-1]
        
        analysis = {
            'current_signal': latest_data['TPD_Signal'],
            'tpd_score': round(latest_data['TPD'], 2),
            'reliability': round(latest_data['TPD_Reliability'], 2),
            'momentum': round(latest_data['TPD_Momentum'], 2),
            'trend': latest_data['TPD_Trend'],
            'risk_level': latest_data['TPD_Risk'],
            'signal_strength': round(latest_data['TPD_Strength'], 2),
            'confluence': round(latest_data['TPD_MTF_Confluence'], 2),
            'divergence': latest_data['TPD_Divergence'],
            'volatility_factor': round(latest_data['TPD_Volatility_Factor'], 2)
        }
        
        # Son 10 periyot sinyal dağılımı
        recent_signals = df['TPD_Signal'].tail(10).value_counts().to_dict()
        analysis['recent_signal_distribution'] = recent_signals
        
        # Sinyal değişim noktaları
        signal_changes = (df['TPD_Signal'] != df['TPD_Signal'].shift(1)).sum()
        analysis['signal_stability'] = 1 - (signal_changes / len(df))
        
        return analysis

    def get_trading_recommendation(self, df: pd.DataFrame) -> Dict:
        """Ticaret önerisi"""
        analysis = self.get_signal_analysis(df)
        
        current_signal = analysis['current_signal']
        reliability = analysis['reliability']
        risk_level = analysis['risk_level']
        signal_strength = analysis['signal_strength']
        
        # Temel öneri
        if current_signal in ['STRONG_BUY', 'BUY']:
            action = 'BUY'
        elif current_signal in ['STRONG_SELL', 'SELL']:
            action = 'SELL'
        elif current_signal in ['WEAK_BUY', 'WEAK_SELL']:
            action = 'WAIT'
        else:
            action = 'NEUTRAL'
        
        # Risk ayarlaması
        if risk_level == 'HIGH':
            if action == 'BUY':
                action = 'CAUTIOUS_BUY'
            elif action == 'SELL':
                action = 'CAUTIOUS_SELL'
        
        # Güvenilirlik ayarlaması
        if reliability < 0.5:
            action = 'WAIT'
        
        # Position sizing önerisi
        if signal_strength > 0.7 and reliability > 0.7:
            position_size = 'NORMAL'
        elif signal_strength > 0.5 and reliability > 0.5:
            position_size = 'REDUCED'
        else:
            position_size = 'MINIMAL'
        
        # Stop loss ve take profit önerileri
        volatility_factor = analysis['volatility_factor']
        if action in ['BUY', 'CAUTIOUS_BUY']:
            stop_loss_pct = 2.0 * volatility_factor
            take_profit_pct = 4.0 * volatility_factor
        elif action in ['SELL', 'CAUTIOUS_SELL']:
            stop_loss_pct = 2.0 * volatility_factor
            take_profit_pct = 4.0 * volatility_factor
        else:
            stop_loss_pct = None
            take_profit_pct = None
        
        recommendation = {
            'action': action,
            'position_size': position_size,
            'confidence': reliability,
            'stop_loss_pct': stop_loss_pct,
            'take_profit_pct': take_profit_pct,
            'risk_level': risk_level,
            'rationale': self._generate_rationale(analysis)
        }
        
        return recommendation

    def _generate_rationale(self, analysis: Dict) -> str:
        """Karar gerekçesi oluştur"""
        rationale_parts = []
        
        # Sinyal durumu
        signal = analysis['current_signal']
        if signal in ['STRONG_BUY', 'BUY']:
            rationale_parts.append("Güçlü alış sinyali mevcut")
        elif signal in ['STRONG_SELL', 'SELL']:
            rationale_parts.append("Güçlü satış sinyali mevcut")
        elif signal in ['WEAK_BUY', 'WEAK_SELL']:
            rationale_parts.append("Zayıf sinyal - dikkatli yaklaşım gerekli")
        
        # Güvenilirlik
        reliability = analysis['reliability']
        if reliability > 0.7:
            rationale_parts.append("Yüksek güvenilirlik")
        elif reliability > 0.5:
            rationale_parts.append("Orta düzey güvenilirlik")
        else:
            rationale_parts.append("Düşük güvenilirlik - risk yüksek")
        
        # Momentum
        momentum = analysis['momentum']
        if abs(momentum) > 3:
            rationale_parts.append("Güçlü momentum")
        elif abs(momentum) > 1:
            rationale_parts.append("Orta momentum")
        else:
            rationale_parts.append("Zayıf momentum")
        
        # Trend
        trend = analysis['trend']
        if trend == 'UPTREND':
            rationale_parts.append("Yükselis trendi")
        elif trend == 'DOWNTREND':
            rationale_parts.append("Düşüş trendi")
        else:
            rationale_parts.append("Yatay trend")
        
        # Risk
        risk = analysis['risk_level']
        if risk == 'HIGH':
            rationale_parts.append("Yüksek risk seviyesi")
        elif risk == 'MEDIUM':
            rationale_parts.append("Orta risk seviyesi")
        else:
            rationale_parts.append("Düşük risk seviyesi")
        
        # Diverjans
        divergence = analysis['divergence']
        if divergence == 'BEARISH_DIVERGENCE':
            rationale_parts.append("Bearish diverjans - dikkat")
        elif divergence == 'BULLISH_DIVERGENCE':
            rationale_parts.append("Bullish diverjans - fırsat")
        
        return " | ".join(rationale_parts)

    def calculate_performance_metrics(self, df: pd.DataFrame) -> Dict:
        """Performans metrikleri hesapla"""
        if 'TPD_Signal' not in df.columns:
            raise ValueError("TPD analizi önce hesaplanmalı")
        
        # Sinyal değişim noktaları
        signal_changes = df['TPD_Signal'] != df['TPD_Signal'].shift(1)
        total_signals = signal_changes.sum()
        
        # Sinyal dağılımı
        signal_distribution = df['TPD_Signal'].value_counts(normalize=True).to_dict()
        
        # Ortalama güvenilirlik
        avg_reliability = df['TPD_Reliability'].mean()
        
        # Ortalama sinyal gücü
        avg_signal_strength = df['TPD_Strength'].mean()
        
        # Volatilite faktörü istatistikleri
        vol_stats = {
            'mean': df['TPD_Volatility_Factor'].mean(),
            'std': df['TPD_Volatility_Factor'].std(),
            'min': df['TPD_Volatility_Factor'].min(),
            'max': df['TPD_Volatility_Factor'].max()
        }
        
        # Risk dağılımı
        risk_distribution = df['TPD_Risk'].value_counts(normalize=True).to_dict()
        
        # Trend dağılımı
        trend_distribution = df['TPD_Trend'].value_counts(normalize=True).to_dict()
        
        # Diverjans istatistikleri
        divergence_stats = df['TPD_Divergence'].value_counts(normalize=True).to_dict()
        
        metrics = {
            'total_signals': total_signals,
            'signal_frequency': total_signals / len(df),
            'signal_distribution': signal_distribution,
            'avg_reliability': round(avg_reliability, 3),
            'avg_signal_strength': round(avg_signal_strength, 3),
            'volatility_stats': vol_stats,
            'risk_distribution': risk_distribution,
            'trend_distribution': trend_distribution,
            'divergence_stats': divergence_stats,
            'data_quality': {
                'total_rows': len(df),
                'null_values': df.isnull().sum().sum(),
                'data_completeness': 1 - (df.isnull().sum().sum() / (len(df) * len(df.columns)))
            }
        }
        
        return metrics
   
class ProfessionalDivergenceDetector:
    """
    Profesyonel seviye divergence tespiti - Risk/Reward düzeltmeleri
    """
    
    def __init__(self, 
                 swing_window: int = 5,
                 min_swing_distance: int = 8,
                 divergence_threshold: float = 0.01,
                 confirmation_candles: int = 2,
                 min_strength: float = 0.05):
        self.swing_window = swing_window
        self.min_swing_distance = min_swing_distance
        self.divergence_threshold = divergence_threshold
        self.confirmation_candles = confirmation_candles
        self.min_strength = min_strength
    
    def find_swing_points(self, data: pd.Series, point_type: str = 'high') -> List[int]:
        """
        Swing noktalarını bulur
        """
        try:
            if len(data) < self.swing_window * 2 + 1:
                return []

            if point_type == 'high':
                peaks = argrelextrema(data.values, np.greater, order=self.swing_window)[0]
            else:
                peaks = argrelextrema(data.values, np.less, order=self.swing_window)[0]

            # Minimum mesafe kontrolü
            filtered_peaks = []
            for peak in peaks:
                if not filtered_peaks:
                    filtered_peaks.append(peak)
                elif peak - filtered_peaks[-1] >= self.min_swing_distance:
                    filtered_peaks.append(peak)
            
            # Son 50 bar içindeki swing noktalarını al
            recent_peaks = [p for p in filtered_peaks if len(data) - p <= 50]
            
            return recent_peaks[-10:] if len(recent_peaks) > 10 else recent_peaks

        except Exception as e:
            print(f"Swing point detection error: {e}")
            return []

    def calculate_dynamic_risk_reward(self, price_data: pd.Series, entry_price: float, 
                                    swing_price: float, divergence_type: str, 
                                    strength: float) -> Dict:
        """
        Dinamik risk/reward hesaplaması
        """
        try:
            # ATR (Average True Range) hesapla - volatilite için
            high_low = price_data.rolling(14).max() - price_data.rolling(14).min()
            atr = high_low.rolling(14).mean().iloc[-1]
            
            # Volatilite bazlı buffer
            volatility_buffer = atr * 0.5  # ATR'nin %50'si
            
            # Divergence tipine göre stop loss ve target hesapla
            if divergence_type in ['regular_bullish', 'hidden_bullish']:
                # Bullish - stop loss swing low'un altında
                stop_loss = swing_price - volatility_buffer
                
                # Target hesaplama - strength ve volatilite bazlı
                risk_distance = abs(entry_price - stop_loss)
                
                # Güç seviyesine göre target multiplier
                if strength > 0.15:
                    target_multiplier = 3.0  # Güçlü sinyal
                elif strength > 0.10:
                    target_multiplier = 2.5  # Orta sinyal
                else:
                    target_multiplier = 2.0  # Zayıf sinyal
                
                target_price = entry_price + (risk_distance * target_multiplier)
                
            else:  # bearish
                # Bearish - stop loss swing high'ın üstünde
                stop_loss = swing_price + volatility_buffer
                
                # Target hesaplama
                risk_distance = abs(stop_loss - entry_price)
                
                # Güç seviyesine göre target multiplier
                if strength > 0.15:
                    target_multiplier = 3.0
                elif strength > 0.10:
                    target_multiplier = 2.5
                else:
                    target_multiplier = 2.0
                
                target_price = entry_price - (risk_distance * target_multiplier)
            
            # Risk ve reward hesapla
            risk = abs(entry_price - stop_loss)
            reward = abs(target_price - entry_price)
            
            if risk > 0:
                rr_ratio = reward / risk
                # Mantıklı sınırlar içinde tut
                rr_ratio = max(0.5, min(rr_ratio, 5.0))
            else:
                rr_ratio = 0
            
            return {
                'risk_reward': round(rr_ratio, 2),
                'entry_price': round(entry_price, 4),
                'stop_loss': round(stop_loss, 4),
                'target_price': round(target_price, 4),
                'risk_amount': round(risk, 4),
                'reward_amount': round(reward, 4),
                'atr': round(atr, 4)
            }
            
        except Exception as e:
            print(f"Risk/reward calculation error: {e}")
            return {'risk_reward': 0}

    def detect_regular_bullish_divergence(self, price_data: pd.Series, indicator_data: pd.Series) -> Dict:
        """
        Regular Bullish Divergence: Fiyat düşük dipler, İndikatör yüksek dipler
        """
        results = {
            'found': False,
            'type': 'regular_bullish',
            'strength': 0,
            'price_points': [],
            'indicator_points': [],
            'confirmation': False,
            'risk_reward': None
        }
        
        try:
            if len(price_data) < 30 or len(indicator_data) < 30:
                return results
            
            # Aynı uzunlukta veri sağla
            min_len = min(len(price_data), len(indicator_data))
            price_data = price_data.iloc[-min_len:].reset_index(drop=True)
            indicator_data = indicator_data.iloc[-min_len:].reset_index(drop=True)
            
            # Fiyat için düşük dipler (lows)
            price_lows = self.find_swing_points(price_data, 'low')
            # İndikatör için düşük dipler (lows) - karşılaştırma için
            indicator_lows = self.find_swing_points(indicator_data, 'low')
            
            if len(price_lows) < 2 or len(indicator_lows) < 2:
                return results
            
            # Son iki swing low'u al
            best_divergence = None
            best_strength = 0
            
            # Farklı kombinasyonları test et
            for i in range(len(price_lows)-1):
                for j in range(i+1, len(price_lows)):
                    price_low1_idx = price_lows[i]
                    price_low2_idx = price_lows[j]
                    
                    # Yakın indicator swing noktalarını bul
                    indicator_low1_idx = min(indicator_lows, key=lambda x: abs(x - price_low1_idx))
                    indicator_low2_idx = min(indicator_lows, key=lambda x: abs(x - price_low2_idx))
                    
                    # Çok uzak swing noktalarını atla
                    if (abs(price_low1_idx - indicator_low1_idx) > 10 or 
                        abs(price_low2_idx - indicator_low2_idx) > 10):
                        continue
                    
                    # Değerleri al
                    price_low1_val = price_data.iloc[price_low1_idx]
                    price_low2_val = price_data.iloc[price_low2_idx]
                    indicator_low1_val = indicator_data.iloc[indicator_low1_idx]
                    indicator_low2_val = indicator_data.iloc[indicator_low2_idx]
                    
                    # Bullish divergence kontrolü
                    # Fiyat: İkinci dip birinci dipten düşük olmalı (lower low)
                    price_makes_lower_low = price_low2_val < price_low1_val
                    # İndikatör: İkinci dip birinci dipten yüksek olmalı (higher low)
                    indicator_makes_higher_low = indicator_low2_val > indicator_low1_val
                    
                    if price_makes_lower_low and indicator_makes_higher_low:
                        # Strength hesapla - daha doğru hesaplama
                        price_change = abs(price_low1_val - price_low2_val) / price_low1_val
                        indicator_change = abs(indicator_low2_val - indicator_low1_val) / abs(indicator_low1_val)
                        
                        # Normalized strength calculation
                        strength = (price_change + indicator_change) / 2
                        
                        if strength > best_strength and strength > self.min_strength:
                            best_strength = strength
                            best_divergence = {
                                'price_points': [(price_low1_idx, price_low1_val), (price_low2_idx, price_low2_val)],
                                'indicator_points': [(indicator_low1_idx, indicator_low1_val), (indicator_low2_idx, indicator_low2_val)],
                                'strength': strength
                            }
            
            if best_divergence:
                results.update(best_divergence)
                results['found'] = True
                
                # Confirmation kontrolü
                if len(price_data) >= self.confirmation_candles:
                    recent_data = price_data.iloc[-self.confirmation_candles:]
                    results['confirmation'] = recent_data.iloc[-1] > recent_data.iloc[0]
                
                # Dinamik Risk/Reward hesaplama
                entry_price = float(price_data.iloc[-1])
                swing_low = float(best_divergence['price_points'][1][1])
                
                # Orijinal price_data'yı geri yükle (reset_index'ten önce)
                original_price_data = price_data  # Bu zaten reset edilmiş
                
                rr_calc = self.calculate_dynamic_risk_reward(
                    original_price_data, entry_price, swing_low, 
                    'regular_bullish', best_strength
                )
                
                results.update(rr_calc)
            
            return results
            
        except Exception as e:
            print(f"Regular bullish divergence error: {e}")
            return results

    def detect_regular_bearish_divergence(self, price_data: pd.Series, indicator_data: pd.Series) -> Dict:
        """
        Regular Bearish Divergence: Fiyat yüksek zirveler, İndikatör düşük zirveler
        """
        results = {
            'found': False,
            'type': 'regular_bearish',
            'strength': 0,
            'price_points': [],
            'indicator_points': [],
            'confirmation': False,
            'risk_reward': None
        }
        
        try:
            if len(price_data) < 30 or len(indicator_data) < 30:
                return results
            
            # Aynı uzunlukta veri sağla
            min_len = min(len(price_data), len(indicator_data))
            price_data = price_data.iloc[-min_len:].reset_index(drop=True)
            indicator_data = indicator_data.iloc[-min_len:].reset_index(drop=True)
            
            # Fiyat için yüksek zirveler (highs)
            price_highs = self.find_swing_points(price_data, 'high')
            # İndikatör için yüksek zirveler (highs)
            indicator_highs = self.find_swing_points(indicator_data, 'high')
            
            if len(price_highs) < 2 or len(indicator_highs) < 2:
                return results
            
            best_divergence = None
            best_strength = 0
            
            # Farklı kombinasyonları test et
            for i in range(len(price_highs)-1):
                for j in range(i+1, len(price_highs)):
                    price_high1_idx = price_highs[i]
                    price_high2_idx = price_highs[j]
                    
                    # Yakın indicator swing noktalarını bul
                    indicator_high1_idx = min(indicator_highs, key=lambda x: abs(x - price_high1_idx))
                    indicator_high2_idx = min(indicator_highs, key=lambda x: abs(x - price_high2_idx))
                    
                    # Çok uzak swing noktalarını atla
                    if (abs(price_high1_idx - indicator_high1_idx) > 10 or 
                        abs(price_high2_idx - indicator_high2_idx) > 10):
                        continue
                    
                    # Değerleri al
                    price_high1_val = price_data.iloc[price_high1_idx]
                    price_high2_val = price_data.iloc[price_high2_idx]
                    indicator_high1_val = indicator_data.iloc[indicator_high1_idx]
                    indicator_high2_val = indicator_data.iloc[indicator_high2_idx]
                    
                    # Bearish divergence kontrolü
                    # Fiyat: İkinci zirve birinci zirveden yüksek olmalı (higher high)
                    price_makes_higher_high = price_high2_val > price_high1_val
                    # İndikatör: İkinci zirve birinci zirveden düşük olmalı (lower high)
                    indicator_makes_lower_high = indicator_high2_val < indicator_high1_val
                    
                    if price_makes_higher_high and indicator_makes_lower_high:
                        # Strength hesapla - daha doğru hesaplama
                        price_change = abs(price_high2_val - price_high1_val) / price_high1_val
                        indicator_change = abs(indicator_high1_val - indicator_high2_val) / abs(indicator_high1_val)
                        
                        # Normalized strength calculation
                        strength = (price_change + indicator_change) / 2
                        
                        if strength > best_strength and strength > self.min_strength:
                            best_strength = strength
                            best_divergence = {
                                'price_points': [(price_high1_idx, price_high1_val), (price_high2_idx, price_high2_val)],
                                'indicator_points': [(indicator_high1_idx, indicator_high1_val), (indicator_high2_idx, indicator_high2_val)],
                                'strength': strength
                            }
            
            if best_divergence:
                results.update(best_divergence)
                results['found'] = True
                
                # Confirmation kontrolü
                if len(price_data) >= self.confirmation_candles:
                    recent_data = price_data.iloc[-self.confirmation_candles:]
                    results['confirmation'] = recent_data.iloc[-1] < recent_data.iloc[0]
                
                # Dinamik Risk/Reward hesaplama
                entry_price = float(price_data.iloc[-1])
                swing_high = float(best_divergence['price_points'][1][1])
                
                rr_calc = self.calculate_dynamic_risk_reward(
                    price_data, entry_price, swing_high, 
                    'regular_bearish', best_strength
                )
                
                results.update(rr_calc)
            
            return results
            
        except Exception as e:
            print(f"Regular bearish divergence error: {e}")
            return results

    def detect_hidden_bullish_divergence(self, price_data: pd.Series, indicator_data: pd.Series) -> Dict:
        """
        Hidden Bullish Divergence: Fiyat yüksek dipler, İndikatör düşük dipler
        """
        results = {
            'found': False,
            'type': 'hidden_bullish',
            'strength': 0,
            'price_points': [],
            'indicator_points': [],
            'confirmation': False,
            'risk_reward': None
        }
        
        try:
            if len(price_data) < 30 or len(indicator_data) < 30:
                return results
            
            # Aynı uzunlukta veri sağla
            min_len = min(len(price_data), len(indicator_data))
            price_data = price_data.iloc[-min_len:].reset_index(drop=True)
            indicator_data = indicator_data.iloc[-min_len:].reset_index(drop=True)
            
            # Fiyat için düşük dipler (lows)
            price_lows = self.find_swing_points(price_data, 'low')
            # İndikatör için düşük dipler (lows)
            indicator_lows = self.find_swing_points(indicator_data, 'low')
            
            if len(price_lows) < 2 or len(indicator_lows) < 2:
                return results
            
            best_divergence = None
            best_strength = 0
            
            # Farklı kombinasyonları test et
            for i in range(len(price_lows)-1):
                for j in range(i+1, len(price_lows)):
                    price_low1_idx = price_lows[i]
                    price_low2_idx = price_lows[j]
                    
                    # Yakın indicator swing noktalarını bul
                    indicator_low1_idx = min(indicator_lows, key=lambda x: abs(x - price_low1_idx))
                    indicator_low2_idx = min(indicator_lows, key=lambda x: abs(x - price_low2_idx))
                    
                    # Çok uzak swing noktalarını atla
                    if (abs(price_low1_idx - indicator_low1_idx) > 10 or 
                        abs(price_low2_idx - indicator_low2_idx) > 10):
                        continue
                    
                    # Değerleri al
                    price_low1_val = price_data.iloc[price_low1_idx]
                    price_low2_val = price_data.iloc[price_low2_idx]
                    indicator_low1_val = indicator_data.iloc[indicator_low1_idx]
                    indicator_low2_val = indicator_data.iloc[indicator_low2_idx]
                    
                    # Hidden bullish divergence kontrolü
                    # Fiyat: İkinci dip birinci dipten yüksek olmalı (higher low)
                    price_makes_higher_low = price_low2_val > price_low1_val
                    # İndikatör: İkinci dip birinci dipten düşük olmalı (lower low)
                    indicator_makes_lower_low = indicator_low2_val < indicator_low1_val
                    
                    if price_makes_higher_low and indicator_makes_lower_low:
                        # Strength hesapla
                        price_change = abs(price_low2_val - price_low1_val) / price_low1_val
                        indicator_change = abs(indicator_low1_val - indicator_low2_val) / abs(indicator_low1_val)
                        strength = (price_change + indicator_change) / 2
                        
                        if strength > best_strength and strength > self.min_strength:
                            best_strength = strength
                            best_divergence = {
                                'price_points': [(price_low1_idx, price_low1_val), (price_low2_idx, price_low2_val)],
                                'indicator_points': [(indicator_low1_idx, indicator_low1_val), (indicator_low2_idx, indicator_low2_val)],
                                'strength': strength
                            }
            
            if best_divergence:
                results.update(best_divergence)
                results['found'] = True
                
                # Confirmation kontrolü
                if len(price_data) >= self.confirmation_candles:
                    recent_data = price_data.iloc[-self.confirmation_candles:]
                    results['confirmation'] = recent_data.iloc[-1] > recent_data.iloc[0]
                
                # Dinamik Risk/Reward hesaplama
                entry_price = float(price_data.iloc[-1])
                swing_low = float(best_divergence['price_points'][0][1])  # İlk düşük dip
                
                rr_calc = self.calculate_dynamic_risk_reward(
                    price_data, entry_price, swing_low, 
                    'hidden_bullish', best_strength
                )
                
                results.update(rr_calc)
            
            return results
            
        except Exception as e:
            print(f"Hidden bullish divergence error: {e}")
            return results

    def detect_hidden_bearish_divergence(self, price_data: pd.Series, indicator_data: pd.Series) -> Dict:
        """
        Hidden Bearish Divergence: Fiyat düşük zirveler, İndikatör yüksek zirveler
        """
        results = {
            'found': False,
            'type': 'hidden_bearish',
            'strength': 0,
            'price_points': [],
            'indicator_points': [],
            'confirmation': False,
            'risk_reward': None
        }
        
        try:
            if len(price_data) < 30 or len(indicator_data) < 30:
                return results
            
            # Aynı uzunlukta veri sağla
            min_len = min(len(price_data), len(indicator_data))
            price_data = price_data.iloc[-min_len:].reset_index(drop=True)
            indicator_data = indicator_data.iloc[-min_len:].reset_index(drop=True)
            
            # Fiyat için yüksek zirveler (highs)
            price_highs = self.find_swing_points(price_data, 'high')
            # İndikatör için yüksek zirveler (highs)
            indicator_highs = self.find_swing_points(indicator_data, 'high')
            
            if len(price_highs) < 2 or len(indicator_highs) < 2:
                return results
            
            best_divergence = None
            best_strength = 0
            
            # Farklı kombinasyonları test et
            for i in range(len(price_highs)-1):
                for j in range(i+1, len(price_highs)):
                    price_high1_idx = price_highs[i]
                    price_high2_idx = price_highs[j]
                    
                    # Yakın indicator swing noktalarını bul
                    indicator_high1_idx = min(indicator_highs, key=lambda x: abs(x - price_high1_idx))
                    indicator_high2_idx = min(indicator_highs, key=lambda x: abs(x - price_high2_idx))
                    
                    # Çok uzak swing noktalarını atla
                    if (abs(price_high1_idx - indicator_high1_idx) > 10 or 
                        abs(price_high2_idx - indicator_high2_idx) > 10):
                        continue
                    
                    # Değerleri al
                    price_high1_val = price_data.iloc[price_high1_idx]
                    price_high2_val = price_data.iloc[price_high2_idx]
                    indicator_high1_val = indicator_data.iloc[indicator_high1_idx]
                    indicator_high2_val = indicator_data.iloc[indicator_high2_idx]
                    
                    # Hidden bearish divergence kontrolü
                    # Fiyat: İkinci zirve birinci zirveden düşük olmalı (lower high)
                    price_makes_lower_high = price_high2_val < price_high1_val
                    # İndikatör: İkinci zirve birinci zirveden yüksek olmalı (higher high)
                    indicator_makes_higher_high = indicator_high2_val > indicator_high1_val
                    
                    if price_makes_lower_high and indicator_makes_higher_high:
                        # Strength hesapla
                        price_change = abs(price_high1_val - price_high2_val) / price_high1_val
                        indicator_change = abs(indicator_high2_val - indicator_high1_val) / abs(indicator_high1_val)
                        strength = (price_change + indicator_change) / 2
                        
                        if strength > best_strength and strength > self.min_strength:
                            best_strength = strength
                            best_divergence = {
                                'price_points': [(price_high1_idx, price_high1_val), (price_high2_idx, price_high2_val)],
                                'indicator_points': [(indicator_high1_idx, indicator_high1_val), (indicator_high2_idx, indicator_high2_val)],
                                'strength': strength
                            }
            
            if best_divergence:
                results.update(best_divergence)
                results['found'] = True
                
                # Confirmation kontrolü
                if len(price_data) >= self.confirmation_candles:
                    recent_data = price_data.iloc[-self.confirmation_candles:]
                    results['confirmation'] = recent_data.iloc[-1] < recent_data.iloc[0]
                
                # Dinamik Risk/Reward hesaplama
                entry_price = float(price_data.iloc[-1])
                swing_high = float(best_divergence['price_points'][0][1])  # İlk yüksek zirve
                
                rr_calc = self.calculate_dynamic_risk_reward(
                    price_data, entry_price, swing_high, 
                    'hidden_bearish', best_strength
                )
                
                results.update(rr_calc)
            
            return results
            
        except Exception as e:
            print(f"Hidden bearish divergence error: {e}")
            return results

    def analyze_multiple_indicators(self, df: pd.DataFrame) -> Dict:
        """
        Çoklu indikatör divergence analizi - Tamamen düzeltilmiş
        """
        results = {
            'regular_bearish': [],
            'regular_bullish': [],
            'hidden_bearish': [],
            'hidden_bullish': [],
            'consensus_signal': None,
            'risk_reward': None,
            'avg_strength': 0
        }
        
        # Kullanılabilir indikatörler - sadece momentum indikatörleri
        indicators = ['RSI', 'MFI', 'MCD', 'RSM', 'SMI']
        price_data = df['close']
        
        all_strengths = []
        
        for indicator in indicators:
            if indicator not in df.columns:
                continue
                
            indicator_data = df[indicator].dropna()
            
            # Minimum veri kontrolü
            if len(indicator_data) < 50:
                continue
            
            # Veri uzunluğunu eşitle
            min_len = min(len(price_data), len(indicator_data))
            if min_len < 50:
                continue
                
            price_subset = price_data.iloc[-min_len:]
            indicator_subset = indicator_data.iloc[-min_len:]
            
            # Regular Bearish Divergence
            bearish_div = self.detect_regular_bearish_divergence(price_subset, indicator_subset)
            if bearish_div['found'] and bearish_div['strength'] > self.min_strength:
                bearish_div['indicator'] = indicator
                results['regular_bearish'].append(bearish_div)
                all_strengths.append(bearish_div['strength'])
            
            # Regular Bullish Divergence
            bullish_div = self.detect_regular_bullish_divergence(price_subset, indicator_subset)
            if bullish_div['found'] and bullish_div['strength'] > self.min_strength:
                bullish_div['indicator'] = indicator
                results['regular_bullish'].append(bullish_div)
                all_strengths.append(bullish_div['strength'])
            
            # Hidden Bullish Divergence
            hidden_bullish = self.detect_hidden_bullish_divergence(price_subset, indicator_subset)
            if hidden_bullish['found'] and hidden_bullish['strength'] > self.min_strength:
                hidden_bullish['indicator'] = indicator
                results['hidden_bullish'].append(hidden_bullish)
                all_strengths.append(hidden_bullish['strength'])
            
            # Hidden Bearish Divergence
            hidden_bearish = self.detect_hidden_bearish_divergence(price_subset, indicator_subset)
            if hidden_bearish['found'] and hidden_bearish['strength'] > self.min_strength:
                hidden_bearish['indicator'] = indicator
                results['hidden_bearish'].append(hidden_bearish)
                all_strengths.append(hidden_bearish['strength'])
        
        # Ortalama güç hesapla
        if all_strengths:
            results['avg_strength'] = sum(all_strengths) / len(all_strengths)
        
        # Consensus Signal belirleme
        regular_bearish_count = len(results['regular_bearish'])
        regular_bullish_count = len(results['regular_bullish'])
        hidden_bearish_count = len(results['hidden_bearish'])
        hidden_bullish_count = len(results['hidden_bullish'])
        
        # Sadece confirmation olan sinyalleri say
        confirmed_bearish = sum(1 for div in results['regular_bearish'] + results['hidden_bearish'] 
                               if div.get('confirmation', False))
        confirmed_bullish = sum(1 for div in results['regular_bullish'] + results['hidden_bullish'] 
                               if div.get('confirmation', False))
        
        # Regular divergence'lar daha güçlü
        total_bearish_score = regular_bearish_count * 2 + hidden_bearish_count
        total_bullish_score = regular_bullish_count * 2 + hidden_bullish_count
        
        if confirmed_bearish >= 2 and total_bearish_score >= 3:
            results['consensus_signal'] = 'STRONG_BEARISH'
        elif confirmed_bullish >= 2 and total_bullish_score >= 3:
            results['consensus_signal'] = 'STRONG_BULLISH'
        elif total_bearish_score >= 2:
            results['consensus_signal'] = 'WEAK_BEARISH'
        elif total_bullish_score >= 2:
            results['consensus_signal'] = 'WEAK_BULLISH'
        else:
            results['consensus_signal'] = 'NEUTRAL'
        
        # En iyi risk/reward oranını bul
        all_divs = (results['regular_bearish'] + results['regular_bullish'] + 
                   results['hidden_bearish'] + results['hidden_bullish'])
        
        if all_divs:
            valid_rr = [d.get('risk_reward', 0) for d in all_divs 
                       if d.get('risk_reward') and d.get('risk_reward') > 0]
            if valid_rr:
                results['risk_reward'] = max(valid_rr)
        
        return results


def format_professional_divergence_output(analysis_result: Dict, interval: str) -> str:
    """
    Profesyonel divergence analiz sonucunu formatla - Tamamen düzeltilmiş
    """
    if not analysis_result:
        return "❓"
    
    consensus = analysis_result.get('consensus_signal', 'NEUTRAL')
    
    # Daha konservatif sinyal verme
    signal_map = {
        'STRONG_BEARISH': '🔴',
        'STRONG_BULLISH': '🟢', 
        'WEAK_BEARISH': '🟠',
        'WEAK_BULLISH': '🟡',
        'NEUTRAL': '➖'
    }
    
    base_signal = signal_map.get(consensus, '❓')
    
    # Detay bilgisi
    reg_bearish = len(analysis_result.get('regular_bearish', []))
    reg_bullish = len(analysis_result.get('regular_bullish', []))
    hid_bearish = len(analysis_result.get('hidden_bearish', []))
    hid_bullish = len(analysis_result.get('hidden_bullish', []))
    
    total_signals = reg_bearish + reg_bullish + hid_bearish + hid_bullish
    
    if total_signals > 0:
        detail = f"R({reg_bearish}/{reg_bullish})H({hid_bearish}/{hid_bullish})"
        
        # Confirmation bilgisi
        confirmed_signals = sum(1 for div_list in [analysis_result.get('regular_bearish', []), 
                                                 analysis_result.get('regular_bullish', []),
                                                 analysis_result.get('hidden_bearish', []), 
                                                 analysis_result.get('hidden_bullish', [])]
                              for div in div_list if div.get('confirmation', False))
        
        if confirmed_signals > 0:
            detail += f"✓{confirmed_signals}"
        
        # Risk/Reward bilgisi
        risk_reward = analysis_result.get('risk_reward')
        if risk_reward and risk_reward > 1:
            detail += f" RR:{risk_reward}"
        
        return f"{base_signal}({detail})"
    
    return base_signal


def get_divergence_display_style(div_value: str, consensus: str) -> tuple:
    """
    Divergence değeri için renk ve stil döndür
    """
    color = 'gray'
    class_name = ''
    
    # Consensus bazlı renklendirme
    if consensus == 'STRONG_BEARISH':
        color = 'red'
        class_name = 'flash strong-signal'  # CSS'de tanımlanmalı
    elif consensus == 'STRONG_BULLISH':
        color = 'green'
        class_name = 'flash strong-signal'
    elif consensus == 'WEAK_BEARISH':
        color = 'orange'
    elif consensus == 'WEAK_BULLISH':
        color = 'lightgreen'
    elif consensus == 'NEUTRAL':
        color = 'gray'
    elif consensus == 'ERROR':
        color = 'darkred'
    elif consensus == 'INSUFFICIENT_DATA':
        color = 'yellow'
    
    # Emoji bazlı ek kontroller
    if '🔴' in div_value:
        color = 'red'
        if '📉' in div_value:
            class_name = 'flash'
    elif '🟢' in div_value:
        color = 'green'
        if '📈' in div_value:
            class_name = 'flash'
    elif '🟠' in div_value:
        color = 'orange'
    elif '❌' in div_value:
        color = 'darkred'
    elif '❓' in div_value:
        color = 'yellow'
    
    return color, class_name


def setup_divergence_cache_optimization():
    """
    Divergence hesaplamaları için cache optimizasyonu
    """
    global divergence_cache
    divergence_cache = {}
    
    def get_cached_analysis(symbol, interval, data_hash):
        cache_key =f" {symbol}-{interval}-{data_hash}"
        return divergence_cache.get(cache_key)
    
    def set_cached_analysis(symbol, interval, data_hash, analysis):
        cache_key =f" {symbol}-{interval}-{data_hash}"
        divergence_cache[cache_key] = analysis
        
        # Cache boyutunu sınırla
        if len(divergence_cache) > 100:
            # En eski 20 kaydı sil
            oldest_keys = list(divergence_cache.keys())[:20]
            for key in oldest_keys:
                del divergence_cache[key]


def calculate_trp(close: pd.Series, length: int = 13) -> pd.Series:
    if len(close) < length:
        return pd.Series([np.nan] * len(close), index=close.index)

    diff = close.diff()
    pos = diff.where(diff > 0, 0.0)
    neg = -diff.where(diff < 0, 0.0)

    pos_sum = pos.rolling(window=length).sum()
    neg_sum = neg.rolling(window=length).sum()

    trp = 100 * (pos_sum - neg_sum) / (pos_sum + neg_sum)
    trp = trp.fillna(0)
    return trp
_sr_cache: dict = {}  # "BTCUSDT-1m" → (support, resistance)

def calculate_support_resistance(df: pd.DataFrame, period: int = 14):
    if df.empty or len(df) < period:
        return None, None
    try:
        recent = df.tail(period)
        return float(recent['low'].min()), float(recent['high'].max())
    except:
        return None, None

def get_sr_signal(close: float, support: float, resistance: float, tol: float = 0.003):
    if close is None or support is None or resistance is None:
        return 'neutral'
    if resistance - support <= 0:
        return 'neutral'
    if close >= resistance * (1 - tol):
        return 'resistance'
    elif close <= support * (1 + tol):
        return 'support'
    elif close > resistance:
        return 'above_res'
    elif close < support:
        return 'below_sup'
    dist_res = (resistance - close) / close
    dist_sup = (close - support) / close
    return 'near_resistance' if dist_res < dist_sup else 'near_support'


def optimize_dataframe(df):
    """DataFrame'i optimize et"""
    # Gereksiz precision'ı azalt
    float_cols = df.select_dtypes(include=[np.float64]).columns
    df[float_cols] = df[float_cols].astype(np.float32)
    
    # Memory usage'ı optimize et
    df = df.copy()
    return df

# 5. Trend kalitesi kontrolü fonksiyonu
def validate_trend_quality(df, min_volatility=0.001):
    """Trend kalitesini kontrol et"""
    if len(df) < 50:
        return False, "Insufficient data"
    
    # Volatilite kontrolü
    volatility = df['close'].pct_change().std()
    if volatility < min_volatility:
        return False, "Low volatility"
    
    # Veri kalitesi kontrolü
    if df['close'].isna().sum() > len(df) * 0.05:
        return False, "Too many missing values"
    
    return True, "Quality OK"

# 6. Trend kuvveti hesaplama fonksiyonu
def calculate_trend_strength(scores):
    """Trend kuvvetini hesapla"""
    if len(scores) < 5:
        return 0
    
    recent_scores = scores[-5:]
    trend_strength = np.mean(recent_scores)
    trend_consistency = 1 - (np.std(recent_scores) / 100)
    
    return trend_strength * trend_consistency

# 7. Sinyal filtreleme fonksiyonu
def filter_signals(scores, threshold=5):
    """Gürültülü sinyalleri filtrele"""
    if len(scores) < 3:
        return scores
    
    filtered = []
    for i, score in enumerate(scores):
        if i < 2:
            filtered.append(score)
        else:
            # Önceki 2 değerle karşılaştır
            prev_avg = np.mean(scores[i-2:i])
            if abs(score - prev_avg) > threshold:
                # Ani değişim varsa yumuşat
                filtered.append(prev_avg * 0.7 + score * 0.3)
            else:
                filtered.append(score)
    
    return filtered

# 8. Backtest fonksiyonu
def backtest_trend_indicator(df, lookback=50):
    """Trend göstergesini backtest et"""
    if len(df) < lookback + 10:
        return {"error": "Insufficient data for backtesting"}
    
    signals = []
    returns = []
    
    for i in range(lookback, len(df)):
        trend_score = df['COMBINED_TREND'].iloc[i]
        
        # Sinyal üret
        if trend_score > 50:
            signal = 1  # Buy
        elif trend_score < -50:
            signal = -1  # Sell
        else:
            signal = 0  # Hold
        
        signals.append(signal)
        
        # Return hesapla
        if i < len(df) - 1:
            ret = (df['close'].iloc[i+1] - df['close'].iloc[i]) / df['close'].iloc[i]
            returns.append(ret * signal)
    
    # Performans metrikleri
    total_return = np.sum(returns)
    win_rate = len([r for r in returns if r > 0]) / len(returns) if returns else 0
    sharpe = np.mean(returns) / np.std(returns) if np.std(returns) > 0 else 0
    
    return {
        "total_return": total_return * 100,
        "win_rate": win_rate * 100,
        "sharpe_ratio": sharpe,
        "total_signals": len(signals)
    }

# 9. Gelişmiş hata yönetimi
def safe_indicator_calculation(func, *args, **kwargs):
    """Güvenli gösterge hesaplama"""
    try:
        return func(*args, **kwargs)
    except Exception as e:
        print(f"Indicator calculation error: {e}")
        return None

# 10. Trend raporu fonksiyonu
def generate_trend_report(df, symbol, interval):
    """Detaylı trend raporu oluştur"""
    if 'COMBINED_TREND' not in df.columns:
        return "No trend data available"
    
    latest_score = df['COMBINED_TREND'].iloc[-1]
    latest_signal = df['COMBINED_TREND_SIGNAL'].iloc[-1]
    
    # Son 24 saat trend analizi
    if len(df) >= 24:
        recent_scores = df['COMBINED_TREND'].iloc[-24:]
        trend_direction = "UP" if recent_scores.iloc[-1] > recent_scores.iloc[0] else "DOWN"
        avg_score = recent_scores.mean()
        volatility = recent_scores.std()
    else:
        trend_direction = "INSUFFICIENT_DATA"
        avg_score = latest_score
        volatility = 0
    
    report = f"""
    📊 TREND REPORT - {symbol} ({interval})
    ═══════════════════════════════════════
    Current Score: {latest_score:.1f}/100
    Signal: {latest_signal}
    Direction: {trend_direction}
    Avg Score (24h): {avg_score:.1f}
    Volatility: {volatility:.1f}
    ═══════════════════════════════════════
    """
    
    return report

async def calculate_indicators_async(df, indicators, interval, symbol):
    """
    Tek bir DataFrame için göstergeleri asenkron olarak hesaplar.
    """
    loop = asyncio.get_event_loop()
    # DataFrame'i listeye çevirip gönderiyoruz ve sonucu direkt olarak alıyoruz
    return await loop.run_in_executor(
        None, 
        lambda: calculate_indicators(df, indicators, interval, symbol)
    )


def calculate_fibonacci_retracement(df: pd.DataFrame, lookback: int = 100) -> dict:
    """
    Son lookback mumundaki high/low arasında Fibonacci seviyeleri hesaplar.
    Dict döndürür — kolon adı → skaler değer.
    """
    try:
        window = df.tail(lookback)
        high = float(window['high'].max())
        low  = float(window['low'].min())
        diff = high - low
        if diff == 0:
            print("Fibonacci hesaplama hatası: high == low, fark sıfır")
            return {}
        levels = [
            ('Fib_0',    0.0),
            ('Fib_236',  23.6),
            ('Fib_382',  38.2),
            ('Fib_500',  50.0),
            ('Fib_618',  61.8),
            ('Fib_786',  78.6),
            ('Fib_1000', 100.0),
        ]
        result = {col: round(high - (diff * pct / 100), 8) for col, pct in levels}
        result['Fib_High'] = high
        result['Fib_Low']  = low
        return result
    except Exception as e:
        print(f"Fibonacci hesaplama hatası: {e}")
        return {}


# ─────────────────────────────────────────────
# KONSENSÜS SİNYAL SİSTEMİ
# Her indikatör oy kullanır → ağırlıklı sonuç
# ─────────────────────────────────────────────

INDICATOR_WEIGHTS = {
    # Trend grubu
    'TRD':  2.0,   # ADX+DI+RSI birleşik — en güvenilir
    'ADX':  1.0,
    'MCD':  1.5,   # MACD
    'EMA':  1.0,
    'SMA_20': 0.5,
    'SMA_50': 0.5,
    'SMA_200':0.5,
    # Momentum grubu
    'RSI':  1.5,
    'SMI':  1.5,
    'WIL':  1.0,
    'CCI':  1.0,
    'RSM':  1.0,
    'TRP':  1.0,
    # Hacim grubu — scalp için ağırlıklı
    'MFI':  1.5,
    'CMF':  1.5,
    'ADO':  1.0,
    'OBV':  0.8,
    'CVD':  1.8,   # momentum+slope bazlı → scalp için güçlü
    'VOL':  2.0,   # z-score bazlı → en anlık sinyal
    'VWAP': 1.5,   # market maker referansı
    # Diğer
    'VRT':  0.8,
    'FRC':  0.8,
    'BOL':  1.0,
}

def _indicator_vote(indicator: str, last_row: dict) -> tuple:
    """
    Her indikatör için (oy, ağırlık) döndürür.
    oy: +1 = BUY, -1 = SELL, 0 = NÖTR
    """
    w = INDICATOR_WEIGHTS.get(indicator, 1.0)

    try:
        if indicator == 'RSI':
            v = last_row.get('RSI', 50)
            if v > 55:   return  1, w
            if v < 45:   return -1, w
            return 0, w

        elif indicator == 'MFI':
            v = last_row.get('MFI', 50)
            if v > 55:   return  1, w
            if v < 45:   return -1, w
            return 0, w

        elif indicator == 'SMI':
            v = last_row.get('SMI', 0)
            if v > 20:   return  1, w
            if v < -20:  return -1, w
            return 0, w

        elif indicator == 'WIL':
            v = last_row.get('WIL', -50)
            if v > -30:  return  1, w   # Aşırı alım bölgesi → güç
            if v < -80:  return -1, w   # Aşırı satım → zayıflık
            return 0, w

        elif indicator == 'CCI':
            v = last_row.get('CCI', 0)
            if v > 100:  return  1, w
            if v < -100: return -1, w
            return 0, w

        elif indicator == 'CMF':
            v = last_row.get('CMF', 0)
            if v > 0.05: return  1, w
            if v < -0.05:return -1, w
            return 0, w

        elif indicator == 'ADO':
            v = last_row.get('ADO', 0)
            if v > 0:    return  1, w
            if v < 0:    return -1, w
            return 0, w

        elif indicator == 'CVD':
            # Kümülatif değer değil, momentum ve slope kullan
            mom   = last_row.get('CVD_Mom', None)
            slope = last_row.get('CVD_Slope', None)
            try:
                mom   = float(mom)   if mom   is not None else None
                slope = float(slope) if slope is not None else None
                if mom   is not None and pd.isna(mom):   mom   = None
                if slope is not None and pd.isna(slope): slope = None
            except:
                mom = slope = None

            if mom is not None and slope is not None:
                if mom > 0 and slope > 0:   return  1, w   # güçlü alış
                if mom < 0 and slope < 0:   return -1, w   # güçlü satış
                if mom > 0:                 return  0.5, w # alış ama yavaşlıyor
                if mom < 0:                 return -0.5, w # satış ama yavaşlıyor
            elif mom is not None:
                if mom > 0: return  1, w
                if mom < 0: return -1, w
            return 0, w

        elif indicator == 'VOL':
            # Z-score varsa onu kullan, yoksa ham değere bak
            z = last_row.get('Net_Volume_Z', None)
            try:
                z = float(z) if z is not None else None
                if z is not None and pd.isna(z): z = None
            except:
                z = None

            if z is not None:
                if z >= 2.0:    return  1, w    # aşırı alım baskısı
                if z <= -2.0:   return -1, w    # aşırı satım baskısı
                if z >= 0.5:    return  0.5, w  # normal alış
                if z <= -0.5:   return -0.5, w  # normal satış
                return 0, w
            else:
                v = last_row.get('Net_Volume', 0)
                if v > 0: return  1, w
                if v < 0: return -1, w
            return 0, w

        elif indicator == 'VWAP':
            close = last_row.get('close', 0)
            vwap  = last_row.get('VWAP', 0)
            if close > vwap * 1.001: return  1, w
            if close < vwap * 0.999: return -1, w
            return 0, w

        elif indicator == 'TRD':
            sig = last_row.get('COMBINED_TREND_SIGNAL', 'WAIT')
            adx = last_row.get('TRD_ADX', 0)
            weight = w * (1.5 if adx >= 40 else 1.0)
            if sig == 'BUY':  return  1, weight
            if sig == 'SELL': return -1, weight
            return 0, w

        elif indicator == 'ADX':
            adx     = last_row.get('ADX', 0)
            plus_di = last_row.get('+DI', 0)
            minus_di= last_row.get('-DI', 0)
            if adx >= 25:
                if plus_di > minus_di: return  1, w
                if minus_di > plus_di: return -1, w
            return 0, w

        elif indicator == 'MCD':
            hist = last_row.get('MCD_Hist', 0)
            if hist > 0:  return  1, w
            if hist < 0:  return -1, w
            return 0, w

        elif indicator == 'EMA':
            sig = last_row.get('EMA_Signal', 0)
            if sig > 0.1:  return  1, w
            if sig < -0.1: return -1, w
            return 0, w

        elif indicator in ['SMA_20', 'SMA_50', 'SMA_200']:
            close = last_row.get('close', 0)
            sma   = last_row.get(indicator, 0)
            if close > sma * 1.001: return  1, w
            if close < sma * 0.999: return -1, w
            return 0, w

        elif indicator == 'BOL':
            close  = last_row.get('close', 0)
            upper  = last_row.get('BB_Upper', float('inf'))
            lower  = last_row.get('BB_Lower', 0)
            middle = last_row.get('BB_Middle', 0)
            if close > middle: return  1, w
            if close < middle: return -1, w
            return 0, w

        elif indicator == 'VRT':
            vi_pos = last_row.get('VI+', 1)
            vi_neg = last_row.get('VI-', 1)
            if vi_pos > vi_neg * 1.05: return  1, w
            if vi_neg > vi_pos * 1.05: return -1, w
            return 0, w

        elif indicator == 'OBV':
            # OBV tek başına trend göstermez, önceki değerle karşılaştır
            return 0, w

        elif indicator == 'RSM':
            v = last_row.get('RSM', 50)
            if v > 55:   return  1, w
            if v < 45:   return -1, w
            return 0, w

        elif indicator == 'TRP':
            v = last_row.get('TRP', 0)
            if v > 0:    return  1, w
            if v < 0:    return -1, w
            return 0, w

        elif indicator == 'FRC':
            v = last_row.get('FRC', 0)
            if v > 0:    return  1, w
            if v < 0:    return -1, w
            return 0, w

    except Exception:
        pass
    return 0, w


def calculate_consensus_signal(df: pd.DataFrame, selected_indicators: List[str]) -> dict:
    """
    Son mumu alır, tüm seçili indikatörleri oylar,
    ağırlıklı skor üretir.
    Döndürür: {score, signal, strength, votes_buy, votes_sell, votes_neutral, details}
    """
    if df is None or df.empty:
        return {'signal': 'NO_DATA', 'score': 0, 'strength': 0}

    last_row = df.iloc[-1].to_dict()
    last_row['close'] = float(last_row.get('close', 0))

    total_weight = 0.0
    weighted_score = 0.0
    votes_buy = 0
    votes_sell = 0
    votes_neutral = 0
    details = {}

    for ind in selected_indicators:
        if ind in ('DIV', 'DIVERGENCE', 'TPD', 'Fibonacci', 'TRD',
                   'TPD_SIGNAL', 'TPD_RELIABILITY', 'TPD_MOMENTUM', 'TPD_RISK',
                   'TPD_CONFLUENCE', 'TPD_DIVERGENCE', 'TPD_STRENGTH', 'TPD_TREND'):
            # TRD'yi konsensüse dahil et ama DIV/TPD gibi karmaşık olanları hariç tut
            if ind != 'TRD':
                continue
        vote, weight = _indicator_vote(ind, last_row)
        weighted_score += vote * weight
        total_weight   += weight
        if vote > 0:   votes_buy     += 1
        elif vote < 0: votes_sell    += 1
        else:          votes_neutral += 1
        details[ind] = vote

    if total_weight == 0:
        return {'signal': 'NO_DATA', 'score': 0, 'strength': 0}

    # -1.0 ile +1.0 arasında normalize edilmiş skor
    normalized = weighted_score / total_weight

    # Sinyal eşikleri
    if normalized >= 0.35:
        signal = 'STRONG_BUY'
    elif normalized >= 0.15:
        signal = 'BUY'
    elif normalized <= -0.35:
        signal = 'STRONG_SELL'
    elif normalized <= -0.15:
        signal = 'SELL'
    else:
        signal = 'NEUTRAL'

    strength = int(abs(normalized) * 100)  # 0-100 arası güç skoru

    return {
        'signal':        signal,
        'score':         round(normalized, 3),
        'strength':      strength,
        'votes_buy':     votes_buy,
        'votes_sell':    votes_sell,
        'votes_neutral': votes_neutral,
        'details':       details,
    }

def calculate_indicators(df: pd.DataFrame, selected_indicators: List[str], interval: str, symbol: str) -> pd.DataFrame:
    if df.empty or len(df) < 30:
        return df

    df = df.copy()
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)

    # talib için zorunlu — object dtype gelirse sessizce fail eder
    for _col in ['open', 'high', 'low', 'close', 'volume']:
        if _col in df.columns and df[_col].dtype != 'float64':
            try:
                df[_col] = df[_col].astype('float64')
            except Exception:
                pass

    close = pd.Series(df['close'].astype('float64'))
    high  = pd.Series(df['high'].astype('float64'))
    low   = pd.Series(df['low'].astype('float64'))
    volume= pd.Series(df['volume'].astype('float64'))

    try:
        # ADX/DI grubunu bir kez işle
        adx_group = {'ADX', '+DI', '-DI'}
        if adx_group & set(selected_indicators):
            df['ADX']  = talib.ADX(high, low, close, timeperiod=14)
            df['+DI']  = talib.PLUS_DI(high, low, close, timeperiod=14)
            df['-DI']  = talib.MINUS_DI(high, low, close, timeperiod=14)

        for indicator in selected_indicators:
            if indicator == 'RSI':
                df['RSI'] = talib.RSI(close, timeperiod=14)

            elif indicator in adx_group:
                pass  # Yukarıda zaten hesaplandı

    
            elif indicator == 'RSM':
                # RSM için özel hesaplama kullanıyoruz
                df['RSM'] = hesapla_rsm(df, periyot=14)['RSM']
            elif indicator == 'MFI':
                df['MFI'] = talib.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod=14)
            
            elif indicator == 'OBV':
                df['OBV'] = ta.volume.OnBalanceVolumeIndicator(close=df['close'], volume=df['volume']).on_balance_volume()
            elif indicator == 'SMI':     
                df['SMI'] = calculate_smi(df)
                df['SMI_Signal'] = df['SMI'].ewm(span=3).mean()
            elif indicator == 'ADO':
                # ADOSC hesaplaması
                df['ADO'] = talib.ADOSC(df['high'], df['low'], df['close'], df['volume'], fastperiod=3, slowperiod=10)
                # Eksik verileri doldur
                df['ADO'] = df['ADO'].fillna(0)
            elif indicator == 'WIL':
                df['WIL'] = talib.WILLR(df['high'], df['low'], df['close'], timeperiod=14)

            elif indicator == 'TRP':
               df['TRP'] = calculate_trp(df['close'], length=13)   

            elif indicator == 'KNS':
                pass  # KNS update_output'ta cache'den hesaplanır, burada skip

            elif indicator == 'CVD':
                # Delta: alış mumunda +volume, satış mumunda -volume
                # close == open → doji, hacmin yarısını her iki tarafa say
                delta = np.where(
                    df['close'] > df['open'],  df['volume'],
                    np.where(df['close'] < df['open'], -df['volume'],
                             0)  # doji → 0 (tartışmalı ama temiz)
                )
                df['CVD_Delta'] = delta

                # Kümülatif CVD (tüm geçmiş — sıfırlanmaz)
                df['CVD'] = df['CVD_Delta'].cumsum()

                # Scalp için kısa pencere momentum: son 20 mumun net deltası
                df['CVD_Mom'] = df['CVD_Delta'].rolling(window=20, min_periods=1).sum()

                # Trend: CVD yükseliyor mu alçalıyor mu (son 5 mumun slope'u)
                df['CVD_Slope'] = df['CVD'].diff(5)

            elif indicator == 'Fibonacci':
                fib_dict = calculate_fibonacci_retracement(df)
                for col, val in fib_dict.items():
                    df[col] = val  # tüm satırlara aynı skaler yaz

            elif indicator == 'CMF':
                cmf = ta.volume.ChaikinMoneyFlowIndicator(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'], window=20)
                df['CMF'] = cmf.chaikin_money_flow()

            elif indicator == 'VRT':  # Changed from 'Vortex' to 'VRT' to match update_output
                vortex = ta.trend.VortexIndicator(high, low, close, window=14)
                df['VI+'] = vortex.vortex_indicator_pos()  # Changed column name to match update_output
                df['VI-'] = vortex.vortex_indicator_neg()  # Changed column name to match update_output

            elif indicator == 'FRC':  # Changed from 'Force' to 'FRC' to match update_output
                df['FRC'] = (close - close.shift(1)) * volume  # Changed column name to match update_output

            elif indicator == 'MCD':  # MACD yerine MCD kullanıldı
                window_fast, window_slow, window_sign = (
                (6, 13, 4) if interval in ['1m', '3m', '5m']
                else (12, 26, 9) if interval in ['15m', '30m', '1h']
                else (24, 52, 18)
                )
                macd = ta.trend.MACD(df['close'], window_fast=window_fast, window_slow=window_slow, window_sign=window_sign)
                df['MCD'] = macd.macd()
                df['MCD_Signal'] = macd.macd_signal()
                df['MCD_Hist'] = macd.macd_diff()

            elif indicator == 'BOL':
                sma = close.rolling(window=20).mean()
                std = close.rolling(window=20).std()
                df['BB_Upper'] = sma + (std * 2)
                df['BB_Middle'] = sma
                df['BB_Lower'] = sma - (std * 2)

            elif indicator == 'EMA':
                # EMA 3 ve EMA 8 hesaplama
                df['EMA_3'] = talib.EMA(df['close'], timeperiod=3)
                df['EMA_8'] = talib.EMA(df['close'], timeperiod=8)
                df['EMA_Signal'] = ((df['EMA_3'] - df['EMA_8']) / df['EMA_8']) * 100
    
        
               
            

            elif indicator == 'CCI':
                # CCI (Commodity Channel Index) hesaplama - 9 periyot scalp için optimize
                df['CCI'] = talib.CCI(df['high'], df['low'], df['close'], timeperiod=9)

            elif indicator == 'VOL':
                # Periyot bazlı Net Volume hesaplama
                # Belirli periyot sayısını zaman dilimine göre ayarla
                if interval == '1m':
                    period = 10  # Son 10 dakika
                elif interval == '3m':
                    period = 10  # Son 30 dakika (10 * 3m)
                elif interval == '5m':
                    period = 12  # Son 60 dakika (12 * 5m)
                elif interval == '15m':
                    period = 8   # Son 2 saat (8 * 15m)
                elif interval == '30m':
                    period = 8   # Son 4 saat (8 * 30m)
                elif interval == '1h':
                    period = 12  # Son 12 saat
                elif interval in ['2h', '4h', '6h', '8h']:
                    period = 12  # Son 24-96 saat
                elif interval == '12h':
                    period = 14  # Son 7 gün
                else:  # 1d
                    period = 30  # Son 30 gün
                
                # Her mum için fiyat değişimini hesapla
                price_change = close.diff()
                
                # Net volume serisi oluştur
                net_volume_series = np.where(
                    price_change > 0, volume,      # Fiyat artışında pozitif
                    np.where(price_change < 0, -volume, 0)  # Fiyat düşüşünde negatif
                )
                
                # Rolling window ile periyot bazlı toplam hesapla
                df['Net_Volume_Raw'] = net_volume_series
                df['Net_Volume'] = df['Net_Volume_Raw'].rolling(window=period, min_periods=1).sum()

            elif indicator == 'SMA_20':
                sma_20 = talib.SMA(df['close'], timeperiod=20)
                df['SMA_20'] = sma_20

            elif indicator == 'SMA_50':
                sma_50 = talib.SMA(df['close'], timeperiod=50) 
                df['SMA_50'] = sma_50

            elif indicator == 'SMA_200':
                sma_200 = talib.SMA(df['close'], timeperiod=200)
                df['SMA_200'] = sma_200

            elif indicator == 'VWAP':
                # Günlük sıfırlanan gerçek VWAP
                try:
                    df['_date'] = pd.to_datetime(df.index).normalize()
                    df['_tp']   = (df['high'] + df['low'] + df['close']) / 3
                    df['_tpv']  = df['_tp'] * df['volume']
                    df['VWAP']  = df.groupby('_date')['_tpv'].cumsum() / df.groupby('_date')['volume'].cumsum()
                    df.drop(columns=['_date', '_tp', '_tpv'], inplace=True)
                except Exception as e:
                    print(f"VWAP hesaplama hatası: {e}")
                    df['VWAP'] = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()

            elif indicator == 'DIV':
                # Zaman dilimlerine özel sonuçları dict olarak al
                divergence_result = detect_professional_divergence_per_interval(df, interval)
                df['DIV'] = divergence_result.get(interval, "✅")

            elif indicator in ('TPD_SIGNAL', 'TPD_RELIABILITY', 'TPD_MOMENTUM', 'TPD_RISK',
                               'TPD_CONFLUENCE', 'TPD_DIVERGENCE', 'TPD_STRENGTH', 'TPD_TREND'):
                try:
                    tpd_indicator = ProfessionalTPDIndicator()
                    df = tpd_indicator.calculate_advanced_tpd(df, interval=interval)
                except Exception as e:
                    print(f"TPD hesaplama hatası ({interval}): {e}")

            elif indicator == 'TRD':
                try:
                    adx_s      = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
                    plus_di_s  = talib.PLUS_DI(df['high'], df['low'], df['close'], timeperiod=14)
                    minus_di_s = talib.MINUS_DI(df['high'], df['low'], df['close'], timeperiod=14)
                    rsi_s      = talib.RSI(df['close'], timeperiod=14)

                    def _calc_trd_signal(a, p, m, r):
                        if pd.isna(a) or pd.isna(p) or pd.isna(m) or pd.isna(r):
                            return 0, 'NO_SIGNAL'
                        if a >= 25:
                            if (p - m >= 5) and (r < 65):
                                return 1, 'BUY'
                            elif (m - p >= 5) and (r > 35):
                                return -1, 'SELL'
                        return 0, 'WAIT'

                    results = [_calc_trd_signal(a, p, m, r)
                               for a, p, m, r in zip(adx_s, plus_di_s, minus_di_s, rsi_s)]

                    df['COMBINED_TREND']        = [r[0] for r in results]
                    df['COMBINED_TREND_SIGNAL'] = [r[1] for r in results]
                    df['TRD_ADX']               = adx_s
                    df['TRD_PLUS_DI']           = plus_di_s   # ← eksikti
                    df['TRD_MINUS_DI']          = minus_di_s  # ← eksikti
                    df['TRD_RSI']               = rsi_s       # ← eksikti

                except Exception as e:
                    print(f"Error in TRD calculation: {e}")
                    df['COMBINED_TREND']        = 0
                    df['COMBINED_TREND_SIGNAL'] = 'ERROR'
                    
            elif indicator == 'SR':
                sup, res = calculate_support_resistance(df, period=14)
                df['SR_Support']    = sup if sup is not None else np.nan
                df['SR_Resistance'] = res if res is not None else np.nan
                key = f"{symbol}-{interval}" if symbol and interval else None
                if key:
                    _sr_cache[key] = (sup, res)

            elif indicator == 'DIVERGENCE':
                # Profesyonel divergence hesaplaması
                if len(df) >= 30:  # Minimum veri kontrolü
                    try:
                        # Detector'ı interval'a göre optimize et
                        detector = ProfessionalDivergenceDetector(
                            swing_window=5 if interval in ['1m', '3m', '5m'] else 7,
                            min_swing_distance=8 if interval in ['1m', '3m', '5m'] else 12,
                            divergence_threshold=0.015 if interval in ['1m', '3m', '5m'] else 0.02,
                            confirmation_candles=3
                        )
                        
                        # Full analiz yap
                        analysis = detector.analyze_multiple_indicators(df)
                        
                        # Ana DIVERGENCE sütunu
                        formatted_result = format_professional_divergence_output(analysis, interval)
                        df['DIVERGENCE'] = formatted_result
                        
                        # Detaylı bilgiler için ek sütunlar (opsiyonel)
                        df['DIV_CONSENSUS'] = analysis.get('consensus_signal', 'NEUTRAL')
                        df['DIV_REG_BEARISH'] = len(analysis.get('regular_bearish', []))
                        df['DIV_REG_BULLISH'] = len(analysis.get('regular_bullish', []))
                        df['DIV_HID_BEARISH'] = len(analysis.get('hidden_bearish', []))
                        df['DIV_HID_BULLISH'] = len(analysis.get('hidden_bullish', []))
                        
                        # Swing noktalarını işaretle (debugging için)
                        if len(analysis.get('regular_bearish', [])) > 0:
                            df['DIV_SWING_HIGH'] = 0
                            for div in analysis['regular_bearish']:
                                for point_idx, point_val in div['price_points']:
                                    if point_idx < len(df):
                                        df.iloc[point_idx, df.columns.get_loc('DIV_SWING_HIGH')] = point_val
                        
                        if len(analysis.get('regular_bullish', [])) > 0:
                            df['DIV_SWING_LOW'] = 0
                            for div in analysis['regular_bullish']:
                                for point_idx, point_val in div['price_points']:
                                    if point_idx < len(df):
                                        df.iloc[point_idx, df.columns.get_loc('DIV_SWING_LOW')] = point_val
                        
                        print(f"✅ Professional divergence calculated for {symbol}-{interval}")
                        
                    except Exception as e:
                        print(f"❌ Professional divergence error for {symbol}-{interval}: {e}")
                        df['DIVERGENCE'] = "❌"
                        df['DIV_CONSENSUS'] = 'ERROR'
                else:
                    # Yetersiz veri
                    df['DIVERGENCE'] = "❓"
                    df['DIV_CONSENSUS'] = 'INSUFFICIENT_DATA'

        # Handle NaN values
        df = df.ffill().fillna(0)

           
    
    except Exception as e:
        print(f"Error calculating indicator: {e}")
         

    return df



def calculate_indicators_in_parallel(dataframes: List[pd.DataFrame], selected_indicators: List[str], interval: str, symbol: str) -> List[pd.DataFrame]:
     #print(f"DEBUG: {symbol} - {interval} için fonksiyon çağrıldı! Veri Seti Uzunluğu: {len(dataframes)}")

    if not dataframes:
         #print("WARNING: DataFrame listesi boş! Hiçbir işlem yapılmayacak.")
        return []

     #print(f"Starting parallel calculation for {symbol} - {interval} with {len(dataframes)} dataframes...")

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(calculate_indicators, df, selected_indicators, interval, symbol): df for df in dataframes}
         #print(f"DEBUG: {len(futures)} iş gönderildi!")

        for future in futures:
            try:
                result = future.result(timeout=5)  # 5 saniye içinde yanıt gelmezse hata ver
                 #print(f"DEBUG: {symbol} - {interval} için iş başarıyla tamamlandı! Veri Uzunluğu={len(futures[future])}")
                results.append(result)
            except Exception as e:
                print(f"ERROR: Paralel işlem sırasında hata oluştu: {e}")
                print(result.head())
                results.append(pd.DataFrame())

    executor.shutdown(wait=True)  

    #print("Parallel calculation completed.")
    return results

def update_indicators(df: pd.DataFrame, selected_indicators: List[str], interval: str, symbol: str) -> pd.DataFrame:
    if df.empty or len(df) < 30:
        return df

        # Sütun tipi karışıklığını önle
    for col in df.columns:
        if df[col].dtype == object:
            try:
                df[col] = pd.to_numeric(df[col], errors='ignore')
            except:
                pass

    cache_key = f"{symbol}-{interval}"
    if cache_key not in cache:
        cache[cache_key] = deque(maxlen=CACHE_SIZE)
        cache[cache_key].append(df.copy())

    close = pd.Series(df['close'])
    high = pd.Series(df['high'])
    low = pd.Series(df['low'])
    volume = pd.Series(df['volume'])
    

    try:
        adx_group = {'ADX', '+DI', '-DI'}
        # ADX grubunu bir kez hesapla
        if adx_group & set(selected_indicators):
            adx_all     = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14)
            plus_di_all = talib.PLUS_DI(df['high'], df['low'], df['close'], timeperiod=14)
            minus_di_all= talib.MINUS_DI(df['high'], df['low'], df['close'], timeperiod=14)
            df.loc[df.index[-1], 'ADX']  = adx_all.iloc[-1]
            df.loc[df.index[-1], '+DI']  = plus_di_all.iloc[-1]
            df.loc[df.index[-1], '-DI']  = minus_di_all.iloc[-1]

        for indicator in selected_indicators:
            if indicator == 'RSI':
                df.loc[df.index[-1], 'RSI'] = talib.RSI(df['close'], timeperiod=14).iloc[-1]

            elif indicator in adx_group:
                pass  # Yukarıda yapıldı

        
            elif indicator == 'ADO':
                ado_values = talib.ADOSC(df['high'], df['low'], df['close'], df['volume'], fastperiod=3, slowperiod=10)
                df.loc[df.index[-1], 'ADO'] = ado_values.iloc[-1]

            elif indicator == 'WIL':
                wıl_values = talib.WILLR(df['high'], df['low'], df['close'], timeperiod=14)
                df.loc[df.index[-1], 'WIL'] = wıl_values.iloc[-1]

            elif indicator == 'MFI':
                mfi_values = talib.MFI(df['high'], df['low'], df['close'], df['volume'], timeperiod=14)
                df.loc[df.index[-1], 'MFI'] = mfi_values.iloc[-1]

            # OBV Güncelleme
            elif indicator == 'OBV':
                obv_values = talib.OBV(df['close'], df['volume'])
                df.loc[df.index[-1], 'OBV'] = obv_values.iloc[-1]

            # SMI Güncelleme (Özel Hesaplama)
            elif indicator == 'SMI':
                smi = calculate_smi(df)
                df.loc[df.index[-1], 'SMI'] = smi.iloc[-1]
                df.loc[df.index[-1], 'SMI_Signal'] = smi.ewm(span=3).mean().iloc[-1]

            # RSM Güncelleme (Özel Hesaplama)
            elif indicator == 'RSM':
                rsm = hesapla_rsm(df, periyot=14)
                df.loc[df.index[-1], 'RSM'] = rsm['RSM'].iloc[-1]

            elif indicator == 'CVD':
                last_close  = df['close'].iloc[-1]
                last_open   = df['open'].iloc[-1]
                last_volume = df['volume'].iloc[-1]

                if last_close > last_open:
                    delta = last_volume
                elif last_close < last_open:
                    delta = -last_volume
                else:
                    delta = 0

                df.loc[df.index[-1], 'CVD_Delta'] = delta

                # Kümülatif CVD: önceki değer + yeni delta (sıfırlanmaz)
                if len(df) > 1 and 'CVD' in df.columns and not pd.isna(df['CVD'].iloc[-2]):
                    df.loc[df.index[-1], 'CVD'] = df['CVD'].iloc[-2] + delta
                else:
                    df.loc[df.index[-1], 'CVD'] = delta

                # Momentum: son 20 mumun delta toplamı
                if 'CVD_Delta' in df.columns:
                    mom_window = min(20, len(df))
                    df.loc[df.index[-1], 'CVD_Mom'] = df['CVD_Delta'].iloc[-mom_window:].sum()

                # Slope: son 5 mumda CVD değişimi
                if 'CVD' in df.columns and len(df) >= 6:
                    df.loc[df.index[-1], 'CVD_Slope'] = df['CVD'].iloc[-1] - df['CVD'].iloc[-6]
                    
            elif indicator == 'CMF':
                cmf = ta.volume.ChaikinMoneyFlowIndicator(high=df['high'], low=df['low'], close=df['close'], volume=df['volume'], window=20)
                df.loc[df.index[-1], 'CMF'] = cmf.chaikin_money_flow().iloc[-1]

            elif indicator == 'VWAP':
                # Günlük sıfırlanan VWAP — son değeri güncelle
                try:
                    df['_date'] = pd.to_datetime(df.index).normalize()
                    df['_tp']   = (df['high'] + df['low'] + df['close']) / 3
                    df['_tpv']  = df['_tp'] * df['volume']
                    df['VWAP']  = df.groupby('_date')['_tpv'].cumsum() / df.groupby('_date')['volume'].cumsum()
                    df.drop(columns=['_date', '_tp', '_tpv'], inplace=True)
                except Exception as e:
                    print(f"VWAP update hatası: {e}")
                    vwap_series = (df['close'] * df['volume']).cumsum() / df['volume'].cumsum()
                    df.loc[df.index[-1], 'VWAP'] = vwap_series.iloc[-1]

            elif indicator == 'Fibonacci':
                # Yeni mum geldiğinde Fibonacci seviyelerini yeniden hesapla
                fib_dict = calculate_fibonacci_retracement(df)
                for col, val in fib_dict.items():
                    df[col] = val  # tüm satırlara sabit skaler yaz

            elif indicator == 'TRP':
                trp_series = calculate_trp(df['close'], length=13)
                df.loc[df.index[-1], 'TRP'] = trp_series.iloc[-1]

            elif indicator == 'VRT':  # Changed from 'Vortex' to 'VRT'
                vortex = ta.trend.VortexIndicator(high, low, close, window=14)
                df.loc[df.index[-1], 'VI+'] = vortex.vortex_indicator_pos().iloc[-1]  # Changed column name
                df.loc[df.index[-1], 'VI-'] = vortex.vortex_indicator_neg().iloc[-1]  # Changed column name

            elif indicator == 'FRC':  # Changed from 'Force' to 'FRC'
                df.loc[df.index[-1], 'FRC'] = (close.iloc[-1] - close.iloc[-2]) * volume.iloc[-1]  # Changed column name

            elif indicator == 'MCD':
                window_fast, window_slow, window_sign = (
                    (6, 13, 4) if interval in ['1m', '3m', '5m']
                    else (12, 26, 9) if interval in ['15m', '30m', '1h']
                    else (24, 52, 18)
                )
                macd = ta.trend.MACD(
                    df['close'],
                    window_fast=window_fast,
                    window_slow=window_slow,
                    window_sign=window_sign
                )
                df['MCD'] = macd.macd()
                df['MCD_Signal'] = macd.macd_signal()
                df['MCD_Hist'] = macd.macd_diff()

            elif indicator == 'BOL':
                sma = close.rolling(window=20).mean()
                std = close.rolling(window=20).std()
                df.loc[df.index[-1], 'BB_Upper'] = sma.iloc[-1] + (std.iloc[-1] * 2)
                df.loc[df.index[-1], 'BB_Middle'] = sma.iloc[-1]
                df.loc[df.index[-1], 'BB_Lower'] = sma.iloc[-1] - (std.iloc[-1] * 2)

            elif indicator == 'EMA':
                # EMA değerlerini hesapla
                ema_3_values = talib.EMA(df['close'], timeperiod=3)
                ema_8_values = talib.EMA(df['close'], timeperiod=8)
                
                # Son değerleri güncelle
                df.loc[df.index[-1], 'EMA_3'] = ema_3_values.iloc[-1]
                df.loc[df.index[-1], 'EMA_8'] = ema_8_values.iloc[-1]
                df.loc[df.index[-1], 'EMA_Signal'] = ((ema_3_values.iloc[-1] - ema_8_values.iloc[-1]) / ema_8_values.iloc[-1]) * 100

            elif indicator == 'CCI':
                # CCI değerlerini hesapla
                cci_values = talib.CCI(df['high'], df['low'], df['close'], timeperiod=9)
                # Son değeri güncelle
                df.loc[df.index[-1], 'CCI'] = cci_values.iloc[-1]

            elif indicator == 'VOL':
                # Periyot sayısını belirle (calculate_indicators ile aynı)
                if interval == '1m':
                    period = 10
                elif interval == '3m':
                    period = 10
                elif interval == '5m':
                    period = 12
                elif interval == '15m':
                    period = 8
                elif interval == '30m':
                    period = 8
                elif interval == '1h':
                    period = 12
                elif interval in ['2h', '4h', '6h', '8h']:
                    period = 12
                elif interval == '12h':
                    period = 14
                else:  # 1d
                    period = 30
                
                if len(close) >= 2:
                    price_change = close.iloc[-1] - close.iloc[-2]
                    if price_change > 0:
                        current_net_volume = volume.iloc[-1]
                    elif price_change < 0:
                        current_net_volume = -volume.iloc[-1]
                    else:
                        current_net_volume = 0
                    
                    df.loc[df.index[-1], 'Net_Volume_Raw'] = current_net_volume
                    
                    if 'Net_Volume_Raw' in df.columns:
                        recent_period = min(period, len(df))
                        rolling_sum = df['Net_Volume_Raw'].iloc[-recent_period:].sum()
                        df.loc[df.index[-1], 'Net_Volume'] = rolling_sum
                    else:
                        rolling_sum = current_net_volume
                        df.loc[df.index[-1], 'Net_Volume'] = current_net_volume

                    # Z-score güncelle
                    if 'Net_Volume' in df.columns and len(df) >= 5:
                        zscore_window = min(100, len(df))
                        nv_series = df['Net_Volume']
                        nv_mean = nv_series.rolling(window=zscore_window, min_periods=5).mean().iloc[-1]
                        nv_std  = nv_series.rolling(window=zscore_window, min_periods=5).std().iloc[-1]
                        if nv_std and nv_std != 0 and not pd.isna(nv_std):
                            df.loc[df.index[-1], 'Net_Volume_Z'] = (rolling_sum - nv_mean) / nv_std
                        else:
                            df.loc[df.index[-1], 'Net_Volume_Z'] = 0
                else:
                    df.loc[df.index[-1], 'Net_Volume_Raw'] = 0
                    df.loc[df.index[-1], 'Net_Volume'] = 0
                    df.loc[df.index[-1], 'Net_Volume_Z'] = 0

            elif indicator == 'SMA_20':
                df.loc[df.index[-1], 'SMA_20'] = talib.SMA(df['close'], timeperiod=20).iloc[-1]
            elif indicator == 'SMA_50':
                df.loc[df.index[-1], 'SMA_50'] = talib.SMA(df['close'], timeperiod=50).iloc[-1]
            elif indicator == 'SMA_200':
                df.loc[df.index[-1], 'SMA_200'] = talib.SMA(df['close'], timeperiod=200).iloc[-1]

            elif indicator in ('TPD_SIGNAL', 'TPD_RELIABILITY', 'TPD_MOMENTUM', 'TPD_RISK',
                               'TPD_CONFLUENCE', 'TPD_DIVERGENCE', 'TPD_STRENGTH', 'TPD_TREND'):
                try:
                    tpd_indicator = ProfessionalTPDIndicator()
                    df = tpd_indicator.calculate_advanced_tpd(df, interval=interval)
                except Exception as e:
                    print(f"TPD güncelleme hatası ({interval}): {e}")

            elif indicator == 'DIV':
                # Zaman dilimlerine özel sonuçları dict olarak al
                divergence_result = detect_professional_divergence_per_interval(df, interval)
                df['DIV'] = divergence_result.get(interval, "✅")

            elif indicator == 'TRD':
                adx_v      = talib.ADX(df['high'], df['low'], df['close'], timeperiod=14).iloc[-1]
                plus_di_v  = talib.PLUS_DI(df['high'], df['low'], df['close'], timeperiod=14).iloc[-1]
                minus_di_v = talib.MINUS_DI(df['high'], df['low'], df['close'], timeperiod=14).iloc[-1]
                rsi_v      = talib.RSI(df['close'], timeperiod=14).iloc[-1]

                if not any(pd.isna(x) for x in [adx_v, plus_di_v, minus_di_v, rsi_v]):
                    if adx_v >= 25:
                        if (plus_di_v - minus_di_v >= 5) and (rsi_v < 65):
                            sig, sig_label = 1, 'BUY'
                        elif (minus_di_v - plus_di_v >= 5) and (rsi_v > 35):
                            sig, sig_label = -1, 'SELL'
                        else:
                            sig, sig_label = 0, 'WAIT'
                    else:
                        sig, sig_label = 0, 'WAIT'
                else:
                    sig, sig_label = 0, 'NO_SIGNAL'

                idx = df.index[-1]
                df.loc[idx, 'COMBINED_TREND']        = sig
                df.loc[idx, 'COMBINED_TREND_SIGNAL'] = sig_label
                df.loc[idx, 'TRD_ADX']               = adx_v
                df.loc[idx, 'TRD_PLUS_DI']           = plus_di_v   # ← eksikti
                df.loc[idx, 'TRD_MINUS_DI']          = minus_di_v  # ← eksikti
                df.loc[idx, 'TRD_RSI']               = rsi_v       # ← eksikti
                
            elif indicator == 'DIVERGENCE':
                # Profesyonel divergence güncelleme
                try:
                    if len(df) >= 30:
                        # Incremental update için optimizasyon
                        # Son 50 mumu analiz et (performans için)
                        analysis_df = df.tail(50).copy() if len(df) > 50 else df.copy()
                        
                        detector = ProfessionalDivergenceDetector(
                            swing_window=5 if interval in ['1m', '3m', '5m'] else 7,
                            min_swing_distance=8 if interval in ['1m', '3m', '5m'] else 12,
                            divergence_threshold=0.015 if interval in ['1m', '3m', '5m'] else 0.02,
                            confirmation_candles=3
                        )
                        
                        # Hızlı analiz (sadece son durumu kontrol et)
                        analysis = detector.analyze_multiple_indicators(analysis_df)
                        formatted_result = format_professional_divergence_output(analysis, interval)
                        
                        # Son satırı güncelle
                        df.loc[df.index[-1], 'DIVERGENCE'] = formatted_result
                        df.loc[df.index[-1], 'DIV_CONSENSUS'] = analysis.get('consensus_signal', 'NEUTRAL')
                        df.loc[df.index[-1], 'DIV_REG_BEARISH'] = len(analysis.get('regular_bearish', []))
                        df.loc[df.index[-1], 'DIV_REG_BULLISH'] = len(analysis.get('regular_bullish', []))
                        df.loc[df.index[-1], 'DIV_HID_BEARISH'] = len(analysis.get('hidden_bearish', []))
                        df.loc[df.index[-1], 'DIV_HID_BULLISH'] = len(analysis.get('hidden_bullish', []))
                        
                        # Alert sistemi (güçlü sinyaller için)
                        consensus = analysis.get('consensus_signal', 'NEUTRAL')
                        if consensus in ['STRONG_BEARISH', 'STRONG_BULLISH']:
                            print(f"🚨 STRONG DIVERGENCE ALERT: {symbol}-{interval} -> {consensus}")
                            
                    else:
                        df.loc[df.index[-1], 'DIVERGENCE'] = "❓"
                        df.loc[df.index[-1], 'DIV_CONSENSUS'] = 'INSUFFICIENT_DATA'
                        
                except Exception as e:
                    print(f"❌ Professional divergence update error: {e}")
                    df.loc[df.index[-1], 'DIVERGENCE'] = "❌"
                    df.loc[df.index[-1], 'DIV_CONSENSUS'] = 'ERROR'
            
        # Handle NaN values
        df.iloc[-1] = df.iloc[-1].ffill().fillna(0)


    except Exception as e:
        print(f"Error updating indicators: {e}")

    return df


def update_column(df, column_name, new_values):
    if column_name not in df.columns:
        df[column_name] = np.nan
    df.loc[df.index[-1], column_name] = new_values.iloc[-1]