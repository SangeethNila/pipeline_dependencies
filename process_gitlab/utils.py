EVAL_REPOS =  ['LINC', 'VLBI']

GIT_ID_TO_NAME = {
        # LINC
        35: 'LINC',
        # imaging compress
        184: 'imaging_compress',
        # VLBI
        362: 'VLBI',
        # solar
        327: 'solar',
        # bf double
        270: 'bf_double',
        # preprocessing
        711: 'preprocessing'
    }

SHORTHAND_TO_PATH = {
        # LINC
        'LINC': "RD\\LINC\\",
        # imaging compress
        'imaging_compress': 'ldv\\imaging_compress_pipeline\\',
        # VLBI
        'VLBI': 'RD\\VLBI-cwl\\',
        # solar
        'solar': 'ssw-ksp\\solar-bf-compressing\\',
        # bf double
        'bf_double': 'ldv\\bf_double_tgz\\',
        # preprocessing
        'preprocessing': 'RD\\preprocessing-cwl\\'
    }

CONSIDERED_MRS = {
        # LINC
        35: [242, 232, 211, 208, 206, 205, 204, 199, 190, 187, 179, 177, 175,
             172, 170, 163, 162, 159, 156, 146, 145, 141, 140, 124, 118, 117,
             101, 93, 92, 91, 89, 88, 86, 76, 72, 63, 31, 30, 28, 20, 19, 18],
        # imaging compress
        184: [1, 2, 3, 4, 10],
        # VLBI
        362: [80, 77, 70, 69, 68, 66, 54, 51, 50, 49,
              48, 45, 42, 
              38, 36, 32, 31, 30, 27, 26, 25, 22, 21, 20,
               19, 18, 17, 16, 14, 13, 9, 6, 5, 3, 1
              ],
        # solar
        327: [1,2,3,4,5],
        # bf double
        270: [7, 5, 2, 1],
        # preprocessing
        711: [14, 5, 4, 3, 1]
    }