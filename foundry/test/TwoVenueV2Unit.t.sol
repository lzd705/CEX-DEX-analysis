// SPDX-License-Identifier: MIT
pragma solidity 0.8.36;

import {Test} from "forge-std/Test.sol";
import {TwoVenueV2Executor} from "../src/TwoVenueV2Executor.sol";

interface ITestToken {
    function allowance(address owner, address spender) external view returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function balanceOf(address account) external view returns (uint256);
    function mint(address account, uint256 amount) external;
    function transfer(address recipient, uint256 amount) external returns (bool);
    function transferFrom(address owner, address recipient, uint256 amount) external returns (bool);
}

contract TestToken {
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;

    function mint(address account, uint256 amount) external {
        balanceOf[account] += amount;
    }

    function approve(address spender, uint256 amount) external returns (bool) {
        allowance[msg.sender][spender] = amount;
        return true;
    }

    function transfer(address recipient, uint256 amount) external returns (bool) {
        balanceOf[msg.sender] -= amount;
        balanceOf[recipient] += amount;
        return true;
    }

    function transferFrom(address owner, address recipient, uint256 amount) external returns (bool) {
        uint256 approved = allowance[owner][msg.sender];
        if (approved != type(uint256).max) {
            allowance[owner][msg.sender] = approved - amount;
        }
        balanceOf[owner] -= amount;
        balanceOf[recipient] += amount;
        return true;
    }
}

contract TestRouter {
    address private constant UNI = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984;
    address private constant WETH_TOKEN = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address private constant UNISWAP_ROUTER = 0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D;
    address private constant UNISWAP_FACTORY = 0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f;
    address private constant SUSHISWAP_FACTORY = 0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac;

    uint256 public callCount;
    uint256 public lastAmountIn;
    uint256 public lastAmountOutMin;
    address public lastPath0;
    address public lastPath1;
    address public lastRecipient;
    uint256 public lastDeadline;
    bool public leaveResidualUni;

    function factory() external view returns (address) {
        return address(this) == UNISWAP_ROUTER ? UNISWAP_FACTORY : SUSHISWAP_FACTORY;
    }

    function WETH() external pure returns (address) {
        return WETH_TOKEN;
    }

    function setLeaveResidualUni(bool enabled) external {
        leaveResidualUni = enabled;
    }

    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256 amountOutMin,
        address[] calldata path,
        address recipient,
        uint256 deadline
    ) external returns (uint256[] memory amounts) {
        require(path.length == 2, "path");
        callCount += 1;
        lastAmountIn = amountIn;
        lastAmountOutMin = amountOutMin;
        lastPath0 = path[0];
        lastPath1 = path[1];
        lastRecipient = recipient;
        lastDeadline = deadline;
        require(
            ITestToken(path[0]).transferFrom(msg.sender, address(this), amountIn),
            "transferFrom"
        );
        uint256 actualOut = amountIn + (address(this) == UNISWAP_ROUTER ? 11 : 22);
        require(ITestToken(path[1]).transfer(recipient, actualOut), "transfer");
        if (leaveResidualUni && path[0] == UNI) {
            ITestToken(UNI).mint(msg.sender, 1);
        }
        amounts = new uint256[](2);
        amounts[0] = amountIn;
        amounts[1] = actualOut + 999;
    }
}

contract BadIdentityRouter {
    function factory() external pure returns (address) {
        return address(0xdead);
    }

    function WETH() external pure returns (address) {
        return address(0xbeef);
    }
}

contract TwoVenueV2UnitTest is Test {
    address private constant UNI = 0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984;
    address private constant WETH = 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2;
    address private constant UNISWAP_ROUTER = 0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D;
    address private constant SUSHISWAP_ROUTER = 0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F;
    address private constant AUTHORIZED_SENDER = 0x5CA9E6c3Ed27Cc0AcFb355061FcaB6964D4Fc444;

    TwoVenueV2Executor private executor;

    function setUp() public {
        vm.etch(UNI, address(new TestToken()).code);
        vm.etch(WETH, address(new TestToken()).code);
        bytes memory routerCode = address(new TestRouter()).code;
        vm.etch(UNISWAP_ROUTER, routerCode);
        vm.etch(SUSHISWAP_ROUTER, routerCode);
        executor = new TwoVenueV2Executor();
        ITestToken(UNI).mint(UNISWAP_ROUTER, 10 ** 30);
        ITestToken(UNI).mint(SUSHISWAP_ROUTER, 10 ** 30);
        ITestToken(WETH).mint(UNISWAP_ROUTER, 10 ** 30);
        ITestToken(WETH).mint(SUSHISWAP_ROUTER, 10 ** 30);
    }

    function testConstructorFreezesReviewedIdentitiesAndApprovals() public view {
        assertEq(ITestToken(UNI).allowance(address(executor), UNISWAP_ROUTER), type(uint256).max);
        assertEq(ITestToken(UNI).allowance(address(executor), SUSHISWAP_ROUTER), type(uint256).max);
        assertEq(ITestToken(WETH).allowance(address(executor), UNISWAP_ROUTER), type(uint256).max);
        assertEq(ITestToken(WETH).allowance(address(executor), SUSHISWAP_ROUTER), type(uint256).max);
    }

    function testConstructorRejectsWrongRouterIdentity() public {
        vm.etch(UNISWAP_ROUTER, address(new BadIdentityRouter()).code);
        vm.expectRevert();
        new TwoVenueV2Executor();
    }

    function testUniswapToSushiUsesActualDeltaAndFixedCallShape() public {
        _assertDirection(TwoVenueV2Executor.Direction.UniswapToSushi, UNISWAP_ROUTER, SUSHISWAP_ROUTER, 11);
    }

    function testSushiToUniswapUsesActualDeltaAndFixedCallShape() public {
        _assertDirection(TwoVenueV2Executor.Direction.SushiToUniswap, SUSHISWAP_ROUTER, UNISWAP_ROUTER, 22);
    }

    function testRejectsUnauthorizedZeroAndInvalidDirectionEncoding() public {
        vm.prank(address(0x1234));
        vm.expectRevert();
        executor.execute(TwoVenueV2Executor.Direction.UniswapToSushi, 1);

        vm.prank(AUTHORIZED_SENDER);
        vm.expectRevert();
        executor.execute(TwoVenueV2Executor.Direction.UniswapToSushi, 0);

        bytes memory encoded = abi.encodeWithSelector(executor.execute.selector, uint256(2), uint256(1));
        vm.prank(AUTHORIZED_SENDER);
        (bool success,) = address(executor).call(encoded);
        assertFalse(success);
    }

    function testRejectsUnexpectedInitialNativeOrTokenState() public {
        ITestToken(WETH).mint(address(executor), 101);
        vm.prank(AUTHORIZED_SENDER);
        vm.expectRevert();
        executor.execute(TwoVenueV2Executor.Direction.UniswapToSushi, 100);

        vm.etch(UNI, address(new TestToken()).code);
        vm.etch(WETH, address(new TestToken()).code);
        executor = new TwoVenueV2Executor();
        ITestToken(WETH).mint(address(executor), 100);
        ITestToken(UNI).mint(address(executor), 1);
        vm.prank(AUTHORIZED_SENDER);
        vm.expectRevert();
        executor.execute(TwoVenueV2Executor.Direction.UniswapToSushi, 100);

        vm.etch(UNI, address(new TestToken()).code);
        vm.etch(WETH, address(new TestToken()).code);
        executor = new TwoVenueV2Executor();
        ITestToken(WETH).mint(address(executor), 100);
        vm.deal(address(executor), 1);
        vm.prank(AUTHORIZED_SENDER);
        vm.expectRevert();
        executor.execute(TwoVenueV2Executor.Direction.UniswapToSushi, 100);
    }

    function testRejectsUnexpectedResidualUni() public {
        uint256 amountIn = 100;
        ITestToken(WETH).mint(address(executor), amountIn);
        TestRouter(SUSHISWAP_ROUTER).setLeaveResidualUni(true);
        vm.prank(AUTHORIZED_SENDER);
        vm.expectRevert();
        executor.execute(TwoVenueV2Executor.Direction.UniswapToSushi, amountIn);
    }

    function testExecutionWritesNoExecutorStorage() public {
        uint256 amountIn = 100;
        ITestToken(WETH).mint(address(executor), amountIn);
        vm.record();
        vm.prank(AUTHORIZED_SENDER);
        executor.execute(TwoVenueV2Executor.Direction.UniswapToSushi, amountIn);
        (, bytes32[] memory writes) = vm.accesses(address(executor));
        assertEq(writes.length, 0);
    }

    function testHasNoReceiveOrFallbackSurface() public {
        vm.deal(address(this), 1 ether);
        (bool nativeSuccess,) = address(executor).call{value: 1}("");
        assertFalse(nativeSuccess);
        (bool fallbackSuccess,) = address(executor).call(hex"ffffffff");
        assertFalse(fallbackSuccess);
    }

    function _assertDirection(
        TwoVenueV2Executor.Direction direction,
        address first,
        address second,
        uint256 firstBonus
    ) private {
        uint256 amountIn = 100;
        uint256 expectedIntermediate = amountIn + firstBonus;
        ITestToken(WETH).mint(address(executor), amountIn);
        vm.warp(123456);
        vm.prank(AUTHORIZED_SENDER);
        (uint256 intermediateUni, uint256 finalWeth) = executor.execute(direction, amountIn);

        assertEq(intermediateUni, expectedIntermediate);
        assertEq(TestRouter(second).lastAmountIn(), expectedIntermediate);
        assertEq(finalWeth, expectedIntermediate + (second == UNISWAP_ROUTER ? 11 : 22));
        assertEq(TestRouter(first).lastAmountOutMin(), 0);
        assertEq(TestRouter(second).lastAmountOutMin(), 0);
        assertEq(TestRouter(first).lastPath0(), WETH);
        assertEq(TestRouter(first).lastPath1(), UNI);
        assertEq(TestRouter(second).lastPath0(), UNI);
        assertEq(TestRouter(second).lastPath1(), WETH);
        assertEq(TestRouter(first).lastRecipient(), address(executor));
        assertEq(TestRouter(second).lastRecipient(), address(executor));
        assertEq(TestRouter(first).lastDeadline(), block.timestamp + 60);
        assertEq(TestRouter(second).lastDeadline(), block.timestamp + 60);
        assertEq(ITestToken(UNI).balanceOf(address(executor)), 0);
        assertEq(ITestToken(WETH).balanceOf(address(executor)), finalWeth);
        assertEq(address(executor).balance, 0);
    }
}
