FROM python:3.10

RUN apt-get update && apt-get install -y libsnappy-dev

# Critical: Force NumPy/SciPy to build without AVX
ENV NPY_DISABLE_CPU_FEATURES="AVX512F,AVX512CD,AVX512_SKX,AVX2,FMA3,AVX"
ENV OPENBLAS_CORETYPE=PRESCOTT

RUN git clone https://github.com/Gin-G/NickKnows.git

WORKDIR NickKnows/app

# Install critical packages from source (not wheels) to respect CPU flags
RUN pip install --no-binary numpy,scipy,scikit-learn numpy scipy scikit-learn

# Now install the rest
RUN pip install -r requirements.txt && \
    pip install typing-extensions --upgrade

CMD [ "python3", "./wsgi.py" ]