// Generated with CPython 3.10.20 from the current Python proposal functions.
// Keep these values fixed: this file is the cross-runtime parity oracle, not
// a second implementation of the scheduling rules.

pub const CONFIG_SHA256: &str =
    "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

pub const PROPOSAL_SEEDS: [(&str, u64, &str); 3] = [
    (
        "zero",
        0,
        "sha256:891de323abbc60bf7de9a28d17e44c4cae70afc8e6220713ca9e3dd738f35ebc",
    ),
    (
        "seven",
        7,
        "sha256:3c0b7cff47f9bcb2285a2e27d30ba03cf0b603ee002621f09644f8949790a7f4",
    ),
    (
        "large",
        123_456_789,
        "sha256:9e43d7c6875fc0c9cb19b88c838ec3800933fac0e2b7558686814d713263dc76",
    ),
];

pub const SELECTORS: [(&str, &str, usize, usize); 5] = [
    ("sha256:abc", "regime", 7, 4),
    ("42", "unicode-🚀", 19, 0),
    ("  🙂  ", "é", 20, 3),
    ("12345678901234567890", "axis", 65_537, 59_657),
    ("emoji 😀", "x\u{001c}y", 97, 77),
];

pub const MUTATION_DEPTHS: [(&str, usize, u8); 4] = [
    ("alpha", 18, 2),
    ("42", 0, 1),
    ("🚀 é", 5, 1),
    ("sha256:abc", 0, 1),
];

pub const NORMAL_IMMIGRANT_SCHEDULE: [bool; 10] = [
    false, false, false, false, true, false, false, false, false, true,
];

pub const ROTATING_TWO_THIRDS_OFFSPRING: [bool; 15] = [
    false, true, true, false, true, true, false, true, true, false, true, true, false, true, true,
];
pub const ROTATING_FOUR_FIFTHS_OFFSPRING: [bool; 15] = [
    false, true, true, true, true, false, true, true, true, true, false, true, true, true, true,
];

pub const SCHEDULE_SHA256: &str =
    "sha256:5427137ff57555b3d2e0b1e4316360de64731873d1947b0f9a896f236d673c36";

pub const GETRANDBITS_BOUNDARIES: [u32; 8] = [0, 1, 2, 31, 32, 33, 63, 64];
pub const RANDBELOW_BOUNDARIES: [u64; 15] = [
    1,
    2,
    3,
    4,
    7,
    8,
    (1_u64 << 31) - 1,
    1_u64 << 31,
    u32::MAX as u64,
    1_u64 << 32,
    (1_u64 << 32) + 1,
    (1_u64 << 63) - 1,
    1_u64 << 63,
    (1_u64 << 63) + 1,
    u64::MAX,
];

pub const MUTATION_CANDIDATE_ID: &str = "qd_pair_345f68f5a37dc4b4ff828d31bee1";
pub const CROSSOVER_SIDE_CANDIDATE_ID: &str = "qd_pair_cross_04e9b0c0cdd23ba6509931d7";
pub const CROSSOVER_PAIR_CANDIDATE_ID: &str = "qd_pair_cross_62318598e0f80a9a9a9023c8";
pub const EXECUTABLE_SEMANTIC_SHA256: &str =
    "sha256:81f027402513beb047a8c2926ffe89afb12cf8baf8bc141143ed96bb1a536376";

pub struct RngOracle {
    pub generation_seed: &'static str,
    pub selection_ordinal: u64,
    pub label: &'static str,
    pub seed_material_sha256: &'static str,
    pub seed: u64,
    pub random_bits: [u64; 5],
    pub getrandbits: [u64; 8],
    pub randbelow: [u64; 15],
}

pub const RNG_ORACLES: [RngOracle; 3] = [
    RngOracle {
        generation_seed: CONFIG_SHA256,
        selection_ordinal: 0,
        label: "parent",
        seed_material_sha256: "sha256:4e72b40cf146f1a2f8cac57c4e2c93e8b431dd68dddcfa5c2312589b1052dc96",
        seed: 5_652_778_449_983_959_458,
        random_bits: [
            4_604_928_205_179_310_610,
            4_603_535_799_153_776_950,
            4_594_809_656_747_021_072,
            4_603_149_355_295_723_394,
            4_585_561_429_423_943_984,
        ],
        getrandbits: [
            0,
            1,
            1,
            1_278_061_760,
            3_602_271_678,
            4_967_502_762,
            5_695_347_389_596_932_564,
            9_380_932_959_514_126_739,
        ],
        randbelow: [
            0,
            0,
            2,
            0,
            4,
            2,
            1_555_456_551,
            1_543_801_613,
            1_102_546_833,
            868_541_628,
            2_242_445_810,
            7_613_698_114_112_258_575,
            1_678_208_988_977_151_723,
            6_404_221_470_604_428_739,
            5_737_827_939_559_921_697,
        ],
    },
    RngOracle {
        generation_seed: CONFIG_SHA256,
        selection_ordinal: 9,
        label: "mate",
        seed_material_sha256: "sha256:920ea06fd8545b65c2a654835adbd995cc9cb4f238e1c9ff0f42c820491ca42c",
        seed: 10_524_525_781_442_648_933,
        random_bits: [
            4_606_269_490_483_873_286,
            4_601_406_728_950_679_914,
            4_606_368_103_608_974_502,
            4_605_801_144_862_088_752,
            4_604_134_124_697_738_926,
        ],
        getrandbits: [
            0,
            1,
            0,
            922_096_863,
            1_179_725_182,
            3_906_671_581,
            3_671_495_858_240_481_485,
            3_978_837_412_135_353_816,
        ],
        randbelow: [
            0,
            1,
            1,
            0,
            6,
            6,
            1_420_713_708,
            926_395_275,
            1_293_830_395,
            2_372_424_153,
            3_839_515_252,
            1_010_592_815_267_225_655,
            1_956_434_273_763_482_577,
            4_891_707_478_887_282_789,
            14_233_296_829_648_414_842,
        ],
    },
    RngOracle {
        generation_seed: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        selection_ordinal: 17,
        label: "parent",
        seed_material_sha256: "sha256:7898a33637bb6f9b05580c546f9720a14a646d1888a724fae1e8098f92e2160b",
        seed: 8_689_874_934_269_964_187,
        random_bits: [
            4_593_777_545_681_802_068,
            4_594_759_648_895_873_932,
            4_604_369_525_995_362_194,
            4_596_285_834_007_271_828,
            4_603_823_156_851_378_757,
        ],
        getrandbits: [
            0,
            0,
            0,
            333_287_034,
            3_419_093_243,
            7_248_642_754,
            3_553_002_264_716_327_501,
            3_891_832_837_837_825_816,
        ],
        randbelow: [
            0,
            0,
            0,
            1,
            3,
            3,
            817_112_372,
            674_826_174,
            4_017_468_095,
            1_217_090_033,
            804_290_564,
            3_289_984_486_400_540_587,
            3_152_416_643_669_011_269,
            7_696_357_702_541_968_160,
            17_527_948_170_845_956_969,
        ],
    },
];
