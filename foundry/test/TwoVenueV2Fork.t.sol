// SPDX-License-Identifier: MIT
pragma solidity 0.8.36;

import {Test} from "forge-std/Test.sol";
import {TwoVenueV2Executor} from "../src/TwoVenueV2Executor.sol";

interface IForkERC20 {
    function balanceOf(address account) external view returns (uint256);
    function decimals() external view returns (uint8);
}

interface IForkRouterV2 {
    function factory() external view returns (address);
    function WETH() external view returns (address);
}

interface IForkFactoryV2 {
    function getPair(address tokenA, address tokenB)
        external
        view
        returns (address);
}

interface IForkPairV2 {
    function getReserves()
        external
        view
        returns (uint112 reserve0, uint112 reserve1, uint32 blockTimestampLast);
    function token0() external view returns (address);
    function token1() external view returns (address);
}

interface IForkAggregatorV3 {
    function decimals() external view returns (uint8);
    function latestRoundData()
        external
        view
        returns (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        );
}

contract TwoVenueV2ForkTest is Test {
    address private constant UNI =
        0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984;
    address private constant WETH =
        0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address private constant UNISWAP_ROUTER =
        0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D;
    address private constant SUSHISWAP_ROUTER =
        0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F;
    address private constant UNISWAP_FACTORY =
        0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f;
    address private constant SUSHISWAP_FACTORY =
        0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac;
    address private constant UNISWAP_PAIR =
        0xd3d2E2692501A5c9Ca623199D38826e513033a17;
    address private constant SUSHISWAP_PAIR =
        0xDafd66636E2561b0284EDdE37e42d192F2844D40;
    address private constant ETH_USD_FEED =
        0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419;
    address private constant AUTHORIZED_SENDER =
        0x5CA9E6c3Ed27Cc0AcFb355061FcaB6964D4Fc444;
    address private constant FORGE_TEST_CONTRACT =
        0x7FA9385bE102ac3EAc297483Dd6233D62b3e1496;
    address private constant REVIEWED_EXECUTOR =
        0x2BD736e245395B754c06d227e2112ACF3e2d401a;
    bytes32 private constant EXECUTOR_CREATE2_SALT =
        0x11b6c7e41f84814790e97cd71e75ce55a4dcd7dd79d7864494a1e45c48bc78c5;

    uint256 private constant REVIEWED_BLOCK_NUMBER = 25_000_000;
    uint256 private constant REVIEWED_BLOCK_TIMESTAMP = 1_777_637_363;
    uint256 private constant ETH_USD_ANSWER = 228_577_572_402;

    uint256 private constant WETH_FOR_USD_1_000 = 437_488_240_640_379_744;
    uint256 private constant WETH_FOR_USD_5_000 = 2_187_441_203_201_898_724;
    uint256 private constant WETH_FOR_USD_10_000 = 4_374_882_406_403_797_449;
    uint256 private constant WETH_FOR_USD_50_000 = 21_874_412_032_018_987_248;
    uint256 private constant WETH_FOR_USD_100_000 = 43_748_824_064_037_974_496;

    TwoVenueV2Executor private executor;

    function setUp() public {
        assertEq(block.chainid, 1);
        assertEq(block.number, REVIEWED_BLOCK_NUMBER);
        assertEq(block.timestamp, REVIEWED_BLOCK_TIMESTAMP);
        assertEq(address(this), FORGE_TEST_CONTRACT);

        _assertCode(UNI);
        _assertCode(WETH);
        _assertCode(UNISWAP_ROUTER);
        _assertCode(SUSHISWAP_ROUTER);
        _assertCode(UNISWAP_FACTORY);
        _assertCode(SUSHISWAP_FACTORY);
        _assertCode(UNISWAP_PAIR);
        _assertCode(SUSHISWAP_PAIR);
        _assertCode(ETH_USD_FEED);

        assertEq(IForkRouterV2(UNISWAP_ROUTER).factory(), UNISWAP_FACTORY);
        assertEq(IForkRouterV2(UNISWAP_ROUTER).WETH(), WETH);
        assertEq(IForkRouterV2(SUSHISWAP_ROUTER).factory(), SUSHISWAP_FACTORY);
        assertEq(IForkRouterV2(SUSHISWAP_ROUTER).WETH(), WETH);

        _assertPair(UNISWAP_FACTORY, UNISWAP_PAIR);
        _assertPair(SUSHISWAP_FACTORY, SUSHISWAP_PAIR);
        assertEq(IForkERC20(UNI).decimals(), 18);
        assertEq(IForkERC20(WETH).decimals(), 18);

        _assertReviewedReserves();
        _assertReviewedFeed();

        address predictedExecutor = address(
            uint160(
                uint256(
                    keccak256(
                        abi.encodePacked(
                            bytes1(0xff),
                            address(this),
                            EXECUTOR_CREATE2_SALT,
                            keccak256(type(TwoVenueV2Executor).creationCode)
                        )
                    )
                )
            )
        );
        assertEq(predictedExecutor, REVIEWED_EXECUTOR);
        assertEq(predictedExecutor.code.length, 0);
        assertEq(predictedExecutor.balance, 0);
        executor =
            new TwoVenueV2Executor{salt: EXECUTOR_CREATE2_SALT}();
        assertEq(address(executor), predictedExecutor);
        _assertCode(predictedExecutor);
        assertEq(predictedExecutor.balance, 0);
    }

    function testUniswapToSushiswapUsd1000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.UniswapToSushi,
            1_000,
            WETH_FOR_USD_1_000
        );
    }

    function testUniswapToSushiswapUsd5000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.UniswapToSushi,
            5_000,
            WETH_FOR_USD_5_000
        );
    }

    function testUniswapToSushiswapUsd10000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.UniswapToSushi,
            10_000,
            WETH_FOR_USD_10_000
        );
    }

    function testUniswapToSushiswapUsd50000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.UniswapToSushi,
            50_000,
            WETH_FOR_USD_50_000
        );
    }

    function testUniswapToSushiswapUsd100000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.UniswapToSushi,
            100_000,
            WETH_FOR_USD_100_000
        );
    }

    function testSushiswapToUniswapUsd1000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.SushiToUniswap,
            1_000,
            WETH_FOR_USD_1_000
        );
    }

    function testSushiswapToUniswapUsd5000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.SushiToUniswap,
            5_000,
            WETH_FOR_USD_5_000
        );
    }

    function testSushiswapToUniswapUsd10000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.SushiToUniswap,
            10_000,
            WETH_FOR_USD_10_000
        );
    }

    function testSushiswapToUniswapUsd50000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.SushiToUniswap,
            50_000,
            WETH_FOR_USD_50_000
        );
    }

    function testSushiswapToUniswapUsd100000() public {
        _runScenario(
            TwoVenueV2Executor.Direction.SushiToUniswap,
            100_000,
            WETH_FOR_USD_100_000
        );
    }

    function _runScenario(
        TwoVenueV2Executor.Direction direction,
        uint256 requestedNotionalUsd,
        uint256 amountWethIn
    ) private {
        assertEq(
            amountWethIn,
            requestedNotionalUsd * 10 ** 26 / ETH_USD_ANSWER
        );

        address firstPair;
        address secondPair;
        if (direction == TwoVenueV2Executor.Direction.UniswapToSushi) {
            firstPair = UNISWAP_PAIR;
            secondPair = SUSHISWAP_PAIR;
        } else {
            firstPair = SUSHISWAP_PAIR;
            secondPair = UNISWAP_PAIR;
        }

        uint256 firstWethBefore = IForkERC20(WETH).balanceOf(firstPair);
        uint256 firstUniBefore = IForkERC20(UNI).balanceOf(firstPair);
        uint256 secondWethBefore = IForkERC20(WETH).balanceOf(secondPair);
        uint256 secondUniBefore = IForkERC20(UNI).balanceOf(secondPair);

        deal(WETH, address(executor), amountWethIn);
        assertEq(IForkERC20(WETH).balanceOf(address(executor)), amountWethIn);
        assertEq(IForkERC20(UNI).balanceOf(address(executor)), 0);
        assertEq(address(executor).balance, 0);

        vm.prank(AUTHORIZED_SENDER);
        (uint256 intermediateUni, uint256 finalWeth) =
            executor.execute(direction, amountWethIn);

        assertGt(intermediateUni, 0);
        assertGt(finalWeth, 0);
        assertEq(
            IForkERC20(WETH).balanceOf(firstPair) - firstWethBefore,
            amountWethIn
        );
        assertEq(
            firstUniBefore - IForkERC20(UNI).balanceOf(firstPair),
            intermediateUni
        );
        assertEq(
            IForkERC20(UNI).balanceOf(secondPair) - secondUniBefore,
            intermediateUni
        );
        assertEq(
            secondWethBefore - IForkERC20(WETH).balanceOf(secondPair),
            finalWeth
        );
        assertEq(IForkERC20(WETH).balanceOf(address(executor)), finalWeth);
        assertEq(IForkERC20(UNI).balanceOf(address(executor)), 0);
        assertEq(address(executor).balance, 0);
        _assertPairSynced(firstPair);
        _assertPairSynced(secondPair);
    }

    function _assertPair(address factory, address expectedPair) private view {
        assertEq(IForkFactoryV2(factory).getPair(UNI, WETH), expectedPair);
        assertEq(IForkFactoryV2(factory).getPair(WETH, UNI), expectedPair);
        assertEq(IForkPairV2(expectedPair).token0(), UNI);
        assertEq(IForkPairV2(expectedPair).token1(), WETH);
    }

    function _assertReviewedReserves() private view {
        (uint112 reserve0, uint112 reserve1, uint32 timestamp) =
            IForkPairV2(UNISWAP_PAIR).getReserves();
        assertEq(uint256(reserve0), 386_708_852_858_506_679_503_887);
        assertEq(uint256(reserve1), 542_990_426_090_335_589_494);
        assertEq(uint256(timestamp), 1_777_635_347);

        (reserve0, reserve1, timestamp) =
            IForkPairV2(SUSHISWAP_PAIR).getReserves();
        assertEq(uint256(reserve0), 3_494_949_632_159_963_323_927);
        assertEq(uint256(reserve1), 4_918_051_786_500_934_660);
        assertEq(uint256(timestamp), 1_777_630_223);
    }

    function _assertReviewedFeed() private view {
        assertEq(IForkAggregatorV3(ETH_USD_FEED).decimals(), 8);
        (
            uint80 roundId,
            int256 answer,
            uint256 startedAt,
            uint256 updatedAt,
            uint80 answeredInRound
        ) = IForkAggregatorV3(ETH_USD_FEED).latestRoundData();
        assertEq(uint256(roundId), 129_127_208_515_966_890_014);
        assertEq(answer, int256(ETH_USD_ANSWER));
        assertEq(startedAt, 1_777_636_927);
        assertEq(updatedAt, 1_777_636_943);
        assertEq(uint256(answeredInRound), 129_127_208_515_966_890_014);
    }

    function _assertPairSynced(address pair) private view {
        (uint112 reserve0, uint112 reserve1,) =
            IForkPairV2(pair).getReserves();
        assertEq(uint256(reserve0), IForkERC20(UNI).balanceOf(pair));
        assertEq(uint256(reserve1), IForkERC20(WETH).balanceOf(pair));
    }

    function _assertCode(address account) private view {
        assertGt(account.code.length, 0);
    }
}
