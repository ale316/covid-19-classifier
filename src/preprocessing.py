import numpy as np
from gammatone import gtgram

class GammatoneFilterbank:
    def __init__(self, 
                 sample_rate, 
                 window_time, 
                 hop_time, 
                 num_filters, 
                 cutoff_low):

        self.sample_rate = sample_rate
        self.window_time = window_time
        self.hop_time = hop_time
        self.num_filters = num_filters
        # Make a spectrogram from a number of audio samples
        # by dividing audio samples into dilated spectral buffers
        self.cutoff_low = cutoff_low

    def make_spectrogram(self, audio_samples):
        return gtgram.gtgram(audio_samples,
                             self.sample_rate,
                             self.window_time,
                             self.hop_time,
                             self.num_filters,
                             self.cutoff_low)

    def make_dilated_spectral_frames(self, 
                                     audio_samples, 
                                     num_frames, 
                                     dilation_factor):

        spectrogram = self.make_spectrogram(audio_samples)
        spectrogram = np.swapaxes(spectrogram, 0, 1)
        dilated_frames = np.zeros((len(spectrogram), 
                                  num_frames, 
                                  len(spectrogram[0])))

        for i in range(len(spectrogram)):
            for j in range(num_frames):
                dilation = np.power(dilation_factor, j)

                if i - dilation < 0:
                    dilated_frames[i][j] = spectrogram[0]
                else:
                    dilated_frames[i][j] = spectrogram[i - dilation]

        return dilated_frames
